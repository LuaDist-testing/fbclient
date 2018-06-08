--[=[
	fbclient library class wrapper.
	based on wrapper.lua and friends.

	*** ATTACHMENTS ***
	attach(database,[username],[password],[client_charset],[role_name],[dpb_options_t],[fbapi_object | libname],[at_class]) -> attachment
	attach_ex(database,[dpb_options_t],[fbapi_object | libname],[at_class]) -> attachment
	create_database(database,[username],[password],[client_charset],[role_name],[db_charset],[page_size],
					[dpb_options_t],[fbapi_object | libname],[at_class]) -> attachmenet
	create_database_ex(database,[dpb_options_t],[fbapi_object | libname],[at_class]) -> attachmenet
	create_database_sql(create_database_sql,[fbapi_object | libname],[at_class]) -> attachmenet
	attachment:clone() -> new attachment on the same fbapi object
	attachment:close()
	attachment:closed() -> true|false
	attachment:drop_database()
	attachment:cancel_operation(cancel_opt_s='fb_cancel_raise')

	*** ATTACHMENT INFO ***
	attachment:database_version() -> {line1,...}
	attachment:info(options_t,[info_buf_len]) -> db_info_t
	attachment:id() -> n
	attachment:page_counts() -> {reads=n,writes=n,fetches=n,marks=n}
	attachment:server_version() -> s
	attachment:page_size() -> n
	attachment:page_count() -> n
	attachment:buffer_count() -> n
	attachment:memory() -> n
	attachment:max_memory() -> n
	attachment:sweep_interval() -> n
	attachment:no_reserve() -> n
	attachment:ods_version() -> {maj,min}
	attachment:forced_writes() -> true|false
	attachment:connected_users() -> {username1,...}
	attachment:read_only() -> true|false
	attachment:creation_date() -> time_t
	attachment:page_contents(page_number) -> s
	attachment:table_counts() -> {[table_id]={read_seq_count=n,read_idx_count=n,...}}

	*** TRANSACTIONS ***
	start_transaction_ex({[attachment1]={tbp_options_t} | true],...},[tr_class]) -> transaction
	attachment:start_transaction_ex([tpb_options_t],[tr_class]) -> transaction
	attachment:start_transaction_sql(set_transaction_sql,[tr_class]) -> transaction
	transaction:commit()
	transaction:rollback()
	transaction:commit_retaining()
	transaction:rollback_retaining()
	transaction:closed() -> true|false
	attachment:commit_all()
	attachment:rollback_all()

	*** TRANSACTION INFO ***
	transaction:info(options_t,[info_buf_len]) -> tr_info_t
	transaction:id() -> n

	*** UNPREPARED STATEMENTS ***
	trasaction:exec_immediate(sql)
	trasaction:exec_immediate_on(attachment,sql)

	*** PREPARED STATEMENTS ***
	transaction:prepare_on(attachment, sql, [st_class]) -> statement
	transaction:prepare(sql, [st_class]) -> statement
	statement:close()
	statement:closed() -> true|false
	statement:run()
	statement:fetch() -> true|false; true = OK, false = EOF.
	statement:close_cursor()
	transaction:close_all_statements()
	attachment:close_all_statements()
	statement:close_all_blobs()

	*** STATEMENT INFO ***
	statement:info(options_t,[info_buf_len]) -> st_info_t
	statement:type() -> type_s
	statement:plan() -> plan_s
	statement:affected_rows() -> {selected=,inserted=,updated=,deleted=}

	*** XSQLVARS ***
	statement.params -> params_t; params_t[i] -> xsqlvar (xsqlvar methods in xsqlvar.lua and friends)
	statement.columns -> columns_t; columns_t[i|alias_name] -> xsqlvar

	*** SUGAR COATING ***
	attachment:exec(sql,p1,p2,...) -> row_iterator() -> i,v1,v2,...
	attachment:exec_immediate(sql)
	transaction:exec_on(attachment,sql,p1,p2,...) -> row_iterator() -> i,v1,v2,...
	transaction:exec(sql,p1,p2,...) -> row_iterator() -> i,v1,v2,...
	statement:exec(v1,v2,...) -> row_iterator() -> i,v1,v2,...
	statement:setparams(v1,v2,...)
	statement:values() -> v1,v2
	statement.values[i|alias_name] -> value
	#statement.values == #statement.params

	*** OBJECT STATE ***
	attachment.fbapi -> fbclient binding object, as returned by fbclient.binding.new(libname)
	attachment.sv -> status_vector object, as returned by fbclient.status_vector.new()
	transaction.fbapi -> fbclient binding object (the fbapi of one of the attachments)
	transaction.sv -> status_vector object (the status vector of one of the attachments)
	statement.fbapi -> fbclient binding object (attachment's fbapi)
	statement.sv -> status_vector object (attachment's sv)

	attachment.transactions -> hash of active transactions on this attachment
	attachment.statements -> hash of active statements on this attachment
	transaction.attachments -> hash of attachments this transaction spans
	transaction.statements -> hash of active statements on this transaction
	statement.attachment -> the attachment this statement executes on
	statement.transaction -> the transaction this statement executes on
	xsqlvar.statement -> statement this xsqlvar object belongs to

	*** CLASS OBJECTS ***
	attachment_class -> table inherited by attachment objects
	transaction_class -> table inherited by transaction objects
	statement_class -> table inherited by statement objects

	*** USAGE NOTES ***
	- auxiliary functionality resides in other modules which are NOT require()'d automatically:
		- blob.lua              xsqlvar methods for blob support
	- see test_class.lua for complete coverage of all the functionality.

	*** LIMITATIONS ***
	- dialect support is burried, and only dialect 3 databases are supported.
	- unpack(statement.values) -> v1,v2,... doesn't work in Lua 5.1 because statement.values is a
	userdata; use statement:values() instead; ipairs(st.values) doesn't work either but you can use
	for i=1,#st.values do ...; neither does pairs(st.values) work but that's less useful.

]=]

module(...,require 'fbclient.init')

local api = require 'fbclient.wrapper' --this module is based on the procedural wrapper
local oo = require 'loop.base' --we use LOOP for classes so you can extend them if you want
local binding = require 'fbclient.binding' --using the wrapper requires a binding object
local svapi = require 'fbclient.status_vector' --using the wrapper requires a status_vector object
local xsqlda = require 'fbclient.xsqlda' --for preallocating xsqlda buffers

attachment_class = oo.class {
	statement_handle_pool_limit = 0, --change this if you're sure to be using fbclient 2.5+
}
transaction_class = oo.class()
statement_class = oo.class()

local function create_attachment_object(fbapi, at_class)
	at_class = at_class or attachment_class
	local at = at_class {
		fbapi = xtype(fbapi) == 'fbclient.binding' and fbapi or binding.new(fbapi or 'fbclient'),
		sv = svapi.new(),
		statement_handle_pool = {},
		prealloc_param_count = 6, --pre-allocate xsqlda on prepare() to avoid a second isc_describe_bind() API call
		prealloc_column_count = 20, --pre-allocate xsqlda on prepare() to avoid a second isc_describe() API call
		transactions = {}, --transactions spanning this attachment
		statements = {}, --statements made against this attachment
	}
	return at
end

function attach(database, user, pass, client_charset, role, opts, fbapi, at_class)
	opts = opts or {}
	opts.isc_dpb_user_name = user
	opts.isc_dpb_password = pass
	opts.isc_dpb_lc_ctype = client_charset
	opts.isc_dpb_sql_role_name = role
	return attach_ex(database, opts, fbapi, at_class)
end

function attach_ex(database, opts, fbapi, at_class)
	local at = create_attachment_object(fbapi, at_class)
	at.handle = api.db_attach(at.fbapi, at.sv, database, opts)
	at.database = database --for cloning
	at.dpb_options = deep_copy(opts) --for cloning
	at.allow_cloning = true
	return at
end

--clone an attachment object reusing the fbapi object of the source attachment
function attachment_class:clone()
	assert(self.handle, 'attachment closed')
	assert(self.allow_cloning, 'cloning not available on this attachment\n'..
								'only attachments made with attach() family of functions can be cloned')
	local at = create_attachment_object(self.fbapi)
	at.database = self.database
	at.dpb_options = deep_copy(self.dpb_options)
	at.allow_cloning = true
	at.handle = api.db_attach(at.fbapi, at.sv, at.database, at.dpb_options)
	return at
end

function create_database(database, user, pass, client_charset, role, db_charset, page_size, opts, fbapi, at_class)
	opts = opts or {}
	opts.isc_dpb_user_name = user
	opts.isc_dpb_password = pass
	opts.isc_dpb_lc_ctype = client_charset
	opts.isc_dpb_sql_role_name = role
	opts.isc_dpb_set_db_sql_dialect = 3
	opts.isc_dpb_set_db_charset = db_charset
	opts.isc_dpb_page_size = page_size
	return create_database_ex(database, opts, fbapi, at_class)
end

function create_database_ex(database, opts, fbapi, at_class)
	local at = create_attachment_object(fbapi, at_class)
	at.handle = api.db_create(at.fbapi, at.sv, database, opts)
	return at
end

function create_database_sql(sql, fbapi, at_class)
	local at = create_attachment_object(fbapi, at_class)
	at.handle = api.db_create_sql(at.fbapi, at.sv, sql, 3)
	return at
end

function attachment_class:close()
	assert(self.handle, 'attachment closed')
	self:rollback_all()
	api.db_detach(self.fbapi, self.sv, self.handle)
	self.handle = nil
end

function attachment_class:drop_database()
	assert(self.handle, 'attachment closed')
	self:rollback_all()
	api.db_drop(self.fbapi, self.sv, self.handle)
	self.handle = nil
end

function attachment_class:cancel_operation(opt)
	assert(self.handle, 'attachment closed')
	opt = opt or 'fb_cancel_raise'
	api.db_cancel_operation(self.fbapi, self.sv, self.handle, opt)
end

function attachment_class:database_version()
	assert(self.handle, 'attachment closed')
	return api.db_version(self.fbapi, self.handle)
end

function attachment_class:info(opts, info_buf_len)
	assert(self.handle, 'attachment closed')
	return api.db_info(self.fbapi, self.sv, self.handle, opts, info_buf_len)
end

function attachment_class:id()
	return self:info({isc_info_attachment_id=true}).isc_info_attachment_id
end

function attachment_class:page_counts()
	local t = self:info{
		isc_info_reads = true,
		isc_info_writes = true,
		isc_info_fetches = true,
		isc_info_marks = true,
	}
	return {
		reads = t.isc_info_reads,
		writes = t.isc_info_writes,
		fetches = t.isc_info_fetches,
		marks = t.isc_info_marks,
	}
end

function attachment_class:server_version()
	return self:info{isc_info_isc_version=true}.isc_info_isc_version
end

function attachment_class:page_size()
	return self:info{isc_info_page_size=true}.isc_info_page_size
end

function attachment_class:page_count()
	return self:info{isc_info_allocation=true}.isc_info_allocation
end

function attachment_class:buffer_count()
	return self:info{isc_info_num_buffers=true}.isc_info_num_buffers
end

function attachment_class:memory()
	return self:info{isc_info_current_memory=true}.isc_info_current_memory
end

function attachment_class:max_memory()
	return self:info{isc_info_max_memory=true}.isc_info_max_memory
end

function attachment_class:sweep_interval()
	return self:info{isc_info_sweep_interval=true}.isc_info_sweep_interval
end

function attachment_class:no_reserve()
	return self:info{isc_info_no_reserve=true}.isc_info_no_reserve
end

function attachment_class:ods_version()
	local t = self:info{
		isc_info_ods_version=true,
		isc_info_ods_minor_version=true,
	}
	return {t.isc_info_ods_version, t.isc_info_ods_minor_version}
end

function attachment_class:forced_writes()
	return self:info{isc_info_forced_writes=true}.isc_info_forced_writes
end

function attachment_class:connected_users()
	return self:info{isc_info_user_names=true}.isc_info_user_names
end

function attachment_class:read_only()
	return self:info{isc_info_db_read_only=true}.isc_info_db_read_only
end

function attachment_class:creation_date()
	return self:info{isc_info_creation_date=true}.isc_info_creation_date
end

function attachment_class:page_contents(page_number)
	return self:info{fb_info_page_contents=page_number}.fb_info_page_contents
end

--returns {[table_id]={read_seq_count=,read_idx_count=,...},...}
function attachment_class:table_counts()
	local qt = {
		isc_info_read_seq_count = true,
		isc_info_read_idx_count = true,
		isc_info_insert_count = true,
		isc_info_update_count = true,
		isc_info_delete_count = true,
		isc_info_backout_count = true,
		isc_info_purge_count = true,
		isc_info_expunge_count = true,
	}
	local t = self:info(qt)
	local rt = {}
	for k in pairs(qt) do
		local kk = k:sub(#'isc_info_'+1)
		for table_id,count in pairs(t[k]) do
			if not rt[table_id] then
				rt[table_id] = {}
			end
			rt[table_id][kk] = count
		end
	end
	return rt
end

--all attachments involved in a multi-database transaction should run on the same OS thread!
function start_transaction_ex(opts, tr_class)
	tr_class = tr_class or transaction_class
	assert(next(opts), 'at least one attachment is necessary to start a transaction')
	local attachments, tpb_opts = {}, {}
	for at,opt in pairs(opts) do
		assert(at.handle, 'attachment closed')
		attachments[at] = true
		tpb_opts[at.handle] = opt
	end
	local tr = tr_class {
		fbapi = next(attachments).fbapi, --use the fbapi of whatever attachment
		sv = next(attachments).sv, --use the sv of whatever attachment
		attachments = attachments,
		statements = {},
	}
	tr.handle = api.tr_start_multiple(tr.fbapi, tr.sv, tpb_opts)
	for at in pairs(attachments) do
		at.transactions[tr] = true
	end
	return tr
end

function attachment_class:start_transaction_ex(opts, tr_class)
	return start_transaction_ex({[self]=opts or true}, tr_class)
end

function attachment_class:start_transaction_sql(sql, tr_class)
	assert(self.handle, 'attachment closed')
	tr_class = tr_class or transaction_class
	local tr = tr_class {
		fbapi = self.fbapi,
		sv = self.sv,
		attachments = {[self] = true},
		statements = {},
	}
	tr.handle = api.tr_start_sql(tr.fbapi, tr.sv, self.handle, sql, 3)
	self.transactions[tr] = true
	return tr
end

function attachment_class:commit_all()
	while next(self.transactions) do
		next(self.transactions):commit()
	end
end

function attachment_class:rollback_all()
	while next(self.transactions) do
		next(self.transactions):rollback()
	end
end

function transaction_class:commit()
	assert(self.handle, 'transaction closed')
	self:close_all_statements()
	api.tr_commit(self.fbapi, self.sv, self.handle)
	for at in pairs(self.attachments) do
		at.transactions[self] = nil
	end
	self.handle = nil
	self.attachments = nil
end

function transaction_class:rollback()
	assert(self.handle, 'transaction closed')
	self:close_all_statements()
	api.tr_rollback(self.fbapi, self.sv, self.handle)
	for at in pairs(self.attachments) do
		at.transactions[self] = nil
	end
	self.handle = nil
	self.attachments = nil
end

function transaction_class:commit_retaining()
	assert(self.handle, 'transaction closed')
	self:close_all_statements()
	api.tr_commit_retaining(self.fbapi, self.sv, self.handle)
end

function transaction_class:rollback_retaining()
	assert(self.handle, 'transaction closed')
	self:close_all_statements()
	api.tr_rollback_retaining(self.fbapi, self.sv, self.handle)
end

function transaction_class:info(opts, info_buf_len)
	assert(self.handle, 'transaction closed')
	return api.tr_info(self.fbapi, self.sv, self.handle, opts, info_buf_len)
end

function transaction_class:id()
	return self:info({isc_info_tra_id=true}).isc_info_tra_id
end

function transaction_class:exec_immediate_on(attachment, sql)
	api.dsql_execute_immediate(self.fbapi, self.sv, attachment.handle, self.handle, sql, 3)
end

function transaction_class:exec_immediate(sql)
	assert(count(self.attachments,2) == 1, 'use exec_immediate_on() on multi-database transactions')
	return self:exec_immediate_on(next(self.attachments), sql)
end

function statement_values_mt_len(t) return #getmetatable(t).statement.columns end
function statement_values_mt_index(t,k) return getmetatable(t).statement.columns[k]:get() end
function statement_values_mt_call(t) return xunpack(t) end

function transaction_class:prepare_on(attachment, sql, st_class)
	assert(self.handle, 'transaction closed')
	st_class = st_class or statement_class
	local st = st_class {
		fbapi = attachment.fbapi,
		sv = attachment.sv,
		transaction = self,
		attachment = attachment,
	}

	--grab a handle from the statement handle pool of the attachment, or make a new one.
	local spool = attachment.statement_handle_pool
	if count(spool,1) > 0 then
		st.handle = next(spool)
		spool[st.handle] = nil
	else
		st.handle = api.dsql_alloc_statement(st.fbapi, st.sv, attachment.handle)
	end

	st.params, st.columns =
		api.dsql_prepare(self.fbapi, self.sv, attachment.handle, self.handle, st.handle, sql, 3,
						xsqlda.new(attachment.prealloc_param_count),
						xsqlda.new(attachment.prealloc_column_count))

	attachment.statements[st] = true
	self.statements[st] = true

	--setup the values virtual table; we make it via newproxy() so we can set __len() on it.
	st.values = newproxy(true)
	local meta = getmetatable(st.values)
	meta.statement = st
	meta.__type = 'fbclient.values'
	meta.__len = statement_values_mt_len
	meta.__index = statement_values_mt_index
	meta.__call = statement_values_mt_call

	return st
end

function transaction_class:prepare(sql, st_class)
	assert(count(self.attachments,2) == 1, 'use prepare_on() on multi-database transactions')
	return self:prepare_on(next(self.attachments), sql, st_class)
end

--NOTE: this function should work fine in the absence of the blob module!
function statement_class:close_all_blobs()
	for i,xs in ipairs(self.params) do
		if xs.blob_handle then
			xs:close()
		end
	end
	for i,xs in ipairs(self.columns) do
		if xs.blob_handle then
			xs:close()
		end
	end
end

function statement_class:close()
	assert(self.handle, 'statement closed')
	self:close_all_blobs()
	self:close_cursor()
	--try unpreparing the statement handle instead of freeing it, and drop it into the handle pool.
	local spool = self.attachment.statement_handle_pool
	local limit = self.attachment.statement_handle_pool_limit
	if count(spool, limit) < limit then
		api.dsql_unprepare(self.fbapi, self.sv, self.handle)
		spool[self.handle] = true
	else
		api.dsql_free_statement(self.fbapi, self.sv, self.handle)
	end

	self.attachment.statements[self] = nil
	self.transaction.statements[self] = nil
	self.handle = nil
	self.transaction = nil
	self.attachment = nil
end

function attachment_class:close_all_statements()
	while next(self.statements) do
		next(self.statements):close()
	end
end

function transaction_class:close_all_statements()
	while next(self.statements) do
		next(self.statements):close()
	end
end

function statement_class:run()
	assert(self.handle, 'statement closed')
	self:close_all_blobs()
	self:close_cursor()
	api.dsql_execute(self.fbapi, self.sv, self.transaction.handle, self.handle, self.params)
	self.cursor_open = true
end

function statement_class:fetch()
	assert(self.handle, 'statement closed')
	self:close_all_blobs()
	local ok = api.dsql_fetch(self.fbapi, self.sv, self.handle, self.columns)
	if not ok then
		self:close_cursor()
	end
	return ok
end

function statement_class:close_cursor()
	if self.cursor_open then
		api.dsql_free_cursor(self.fbapi, self.sv, self.handle)
		self.cursor_open = nil
	end
end

function statement_class:info(opts, info_buf_len)
	assert(self.handle, 'statement closed')
	return api.dsql_info(self.fbapi, self.sv, self.handle, opts, info_buf_len)
end

function statement_class:type()
	return self:info({isc_info_sql_stmt_type=true}).isc_info_sql_stmt_type[1]
end

function statement_class:plan()
	return self:info({isc_info_sql_get_plan=true}).isc_info_sql_get_plan
end

local affected_rows_codes = {
	isc_info_req_select_count = 'selects',
	isc_info_req_insert_count = 'inserts',
	isc_info_req_update_count = 'updates',
	isc_info_req_delete_count = 'deletes',
}

function statement_class:affected_rows()
	local t, codes = {}, self:info({isc_info_sql_records=true}).isc_info_sql_records
	for k,v in pairs(codes) do
		t[affected_rows_codes[k]] = v
	end
	return t
end

function statement_class:setparams(...)
	for i,p in ipairs(self.params) do
		p:set(select(i,...))
	end
end

local function statement_exec_iter(st,i)
	if st:fetch() then
		return i+1, st:values()
	end
end

function statement_class:exec(...)
	self:setparams(...)
	self:run()
	return statement_exec_iter,self,0
end

local function transaction_exec_iter(st)
	if st:fetch() then
		return st, st:values()
	else
		st:close()
	end
end

--ATTN: if you break the iteration, either with break or with error(), the statement and cursor remain open
--until closing of the transaction!
function transaction_class:exec_on(at,sql,...)
	local st = self:prepare_on(at,sql)
	st:setparams(...)
	st:run()
	return transaction_exec_iter,st
end

function transaction_class:exec(sql,...)
	local st = self:prepare(sql)
	st:setparams(...)
	st:run()
	return transaction_exec_iter,st
end

local function attachment_exec_iter(st)
	if st:fetch() then
		return st, st:values()
	else
		st.transaction:commit() --commit() closes all statements automatically
	end
end

--ATTN: if you break the iteration, either with break or with error(), the transaction, statement and cursor
--all remain open until closing of the attachment!
function attachment_class:exec(sql,...)
	local tr = self:start_transaction_ex()
	local st = tr:prepare_on(self,sql)
	st:setparams(...)
	st:run()
	return attachment_exec_iter,st
end

function attachment_class:exec_immediate(sql)
	local tr = self:start_transaction_ex()
	tr:exec_immediate_on(self,sql)
	tr:commit()
end

local function object_closed(self)
	return self.handle == nil
end

attachment_class.closed = object_closed
transaction_class.closed = object_closed
statement_class.closed = object_closed

