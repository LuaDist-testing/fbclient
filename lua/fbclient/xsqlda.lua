--[=[
	XSQLDA & XSQLVAR structures: the record & field buffers.

	new(xsqlvar_count) -> xsqlda_buf
	decode(xsqlda_buf) -> xsqlvar_allocated_count, xsqlvar_used_count
	xsqlvars(xsqlda_buf) -> iterator() -> i,xsqlvar_ptr
	xsqlvar_alloc(xsqlvar_ptr, [xs_meta]) -> xsqlvar_t

	xsqlvar.column_name
	xsqlvar.table_name
	xsqlvar.table_owner_name
	xsqlvar.column_alias_name
	xsqlvar.fbapi -> fbapi (binding) object
	xsqlvar.sv -> status vector
	xsqlvar.dbh -> database handle
	xsqlvar.trh -> transaction handle

	USAGE: to become an xsqlvar object, the xsqlvar_t must be wrapped with xsqlvar.wrap() (see wrapper.lua).

	LIMITATIONS:
	- doesn't support setting undescribed xsqlvars, needed for isc_dqsl_execute_immediate().

]=]

module(...,require 'fbclient.init')

local sqltypes = {
	SQL_TEXT        = 452,
	SQL_VARYING     = 448,
	SQL_SHORT       = 500,
	SQL_LONG        = 496,
	SQL_FLOAT       = 482,
	SQL_DOUBLE      = 480,
	SQL_D_FLOAT     = 530,
	SQL_TIMESTAMP   = 510,
	SQL_BLOB        = 520,
	SQL_ARRAY       = 540,
	SQL_QUAD        = 550,
	SQL_TYPE_TIME   = 560,
	SQL_TYPE_DATE   = 570,
	SQL_INT64       = 580,
}

local sqlsubtypes = {
	isc_blob_untyped    = 0,
	isc_blob_text       = 1,
	isc_blob_blr        = 2,
	isc_blob_acl        = 3,
	isc_blob_ranges     = 4,
	isc_blob_summary    = 5,
	isc_blob_format     = 6,
	isc_blob_tra        = 7,
	isc_blob_extfile    = 8,
	isc_blob_debug_info = 9,
}

local sqltype_lookup = index(sqltypes)
local sqlsubtype_lookup = index(sqlsubtypes)

--version,sqlda_name,sqlda_byte_count,xsqlvar_allocated_count,xsqlvar_used_count,XSQLVAR1,XSQLVAR2,...
local XSQLDA = '!4hc8ihh'
local ISC_NAME = 'hc32'-- only 32 lousy characters, so '80s
--type,scale,subtype,len,sqldata*,sqlind*,#sqlname,sqlname,#relname,relname,#ownname,ownname,#aliasname,aliasname
local XSQLVAR = '!4hhhhpp'..ISC_NAME..ISC_NAME..ISC_NAME..ISC_NAME
local XSQLVAR_SQLDATA_OFFSET = struct.offset(XSQLVAR,5)
local XSQLVAR_SQLIND_OFFSET  = struct.offset(XSQLVAR,6)

-- you need these for most of dsql_*() functions. so let's get you one.
function new(xsqlvar_count)
	assert(xsqlvar_count >= 0)
	local buf = alien.buffer(struct.size(XSQLDA)+xsqlvar_count*struct.size(XSQLVAR))
	alien.memcpy(buf, struct.pack(XSQLDA, 1, ('\0'):rep(8), 0, xsqlvar_count, 0))
	return buf
end

-- call this after isc_dsql_prepare() or isc_dsql_describe*() to find out how many actual
-- columns/parameters are needed. if used_count < alloc_count, you'll need to reallocate the xsqlda.
function decode(xsqlda_buf)
	local
		version,			-- 1
		sqlda_name,			-- reserved
		sqlda_byte_count,	-- reserved
		xsqlvar_allocated_count,
		xsqlvar_used_count = struct.unpack(XSQLDA, xsqlda_buf, struct.size(XSQLDA))

	return xsqlvar_allocated_count, xsqlvar_used_count
end

-- returns a reusable, self-contained iterator of i,next_xsqlvar_ptr (which is a pointer inside the xsqlda)
function xsqlvars(xsqlda_buf)
	local alloc, used = decode(xsqlda_buf)
	assert(used <= alloc) -- can't iterate unless there are enough buffers
	return function(_,i)
		i=i and i+1 or 1
		if i > used then
			return nil
		else
			return i,xsqlda_buf:topointer(struct.size(XSQLDA)+(i-1)*struct.size(XSQLVAR)+1)
		end
	end
end

-- internal hepler that computes buflen for a certain sqltype,sqllen
local function sqldata_buflen(sqltype, sqllen)
	local buflen = sqllen
	if sqltype == 'SQL_VARYING' then
		buflen = sqllen+SHORT_SIZE
	end
	return buflen
end

--call this after isc_dsql_prepare() or isc_dsql_describe*() for each xsqlvar of an xsqlda
--to decode the xsqlvar structure and allocate the sqlda/sqlind buffers accoding to the xsqlvar type.
--to further decode the returned table, see xsqlvar.lua and wrapper.lua.
--NOTE: since the XSQLDA buffer contains a pointer to the SQLDATA and SQLIND buffers that are part of
--the xsqlvar_t, you have to keep the xsqlvar_t from garbage-collecting!
function xsqlvar_alloc(xsqlvar_ptr)
	local sqltype, sqlscale, subtype, sqllen, _, _,
	len_sqlname, sqlname,
	len_relname, relname,
	len_ownname, ownname,
	len_aliasname, aliasname = struct.unpack(XSQLVAR, xsqlvar_ptr, struct.size(XSQLVAR))

	-- allow_null tells you if you'll have to allocate an sqlind buffer or not (for tube computers)
	local allow_null = sqltype%2==1 -- yea, odd (that is, the flag is kept in bit 1)
	sqltype = sqltype_lookup[sqltype - (allow_null and 1 or 0)]
	assert(sqltype)
	if sqltype == 'SQL_BLOB' then
		subtype = sqlsubtype_lookup[subtype]
		assert(subtype)
	else
		subtype = nil
	end

	sqlname = len_sqlname > 0 and sqlname:sub(1,len_sqlname) or nil
	relname = len_relname > 0 and relname:sub(1,len_relname) or nil
	ownname = len_ownname > 0 and ownname:sub(1,len_ownname) or nil
	aliasname = len_aliasname > 0 and aliasname:sub(1,len_aliasname) or nil

	local buflen = sqldata_buflen(sqltype, sqllen)
	assert(buflen > 0)
	local sqldata_buf = alien.buffer(buflen)
	local sqlind_buf
	if allow_null then
		sqlind_buf = alien.buffer(INT_SIZE)
		sqlind_buf:set(1,-1,'int') --initialize to null
	else
		alien.memset(sqldata_buf,0,buflen) --initialize to zero (less ideal than null)
	end

	--TODO: I need to write to a pointer at an offset directly without making garbage!
	local tmpbuf = alien.buffer(xsqlvar_ptr)
	tmpbuf:set(XSQLVAR_SQLDATA_OFFSET, sqldata_buf, 'pointer')
	tmpbuf:set(XSQLVAR_SQLIND_OFFSET, sqlind_buf, 'pointer')

	local xs = {
		sqltype = sqltype, --how is SQLDATA encoded
		sqlscale = sqlscale, --for numbers obviously
		sqllen = sqllen, --max. size of the *contents* of the SQLDATA buffer
		buflen = buflen, --size of the SQLDATA buffer
		subtype = subtype, --how is a blob encoded
		allow_null = allow_null, --should we allocate an sqlind buffer or not
		sqldata_buf = sqldata_buf, --SQLDATA buffer
		sqlind_buf = sqlind_buf,  --SQLIND buffer
		column_name = sqlname,
		table_name = relname,
		table_owner_name = ownname,
		column_alias_name = aliasname,
	}
	return xs
end

