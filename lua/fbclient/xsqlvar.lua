--[=[
	SQLDATA & SQLIND buffer encoding and decoding.

	xsqlvar_class -> the table that xsqlvar objects inherit
	xsqlvar_meta -> the metatable of xsqlvar objects
	xsqlvar_meta.__index = xsqlvar_class
	xsqlvar_meta.__type = 'fbclient.xsqlvar'

	wrap(xsqlvar_t, fbapi, sv, dbh, trh, [xsqlvar_meta]) -> xsqlvar

	xsqlvar:allownull() -> true|false
	xsqlvar:isnull() -> true|false
	xsqlvar:setnull()
	xsqlvar:type() -> type,subtype; subtype is scale for numbers, max_length for strings, subtype for blobs.

	xsqlvar:gettime() -> time_t           for DATE, TIME, TIMESTAMP
	xsqlvar:settime(time_t)               for DATE, TIME, TIMESTAMP; see note (1)
	xsqlvar:getnumber() -> n              for FLOAT, DOUBLE PRECISION, SMALLINT, INTEGER, DECIMAL(1-15,0), BIGINT < +/-2^52-1
	xsqlvar:setnumber(n)                  for FLOAT, DOUBLE PRECISION, SMALLINT, INTEGER, DECIMAL(1-15,0), BIGINT < +/-2^52-1
	xsqlvar:getparts() -> parts_t         for SMALLINT, INTEGER, DECIMAL(1-15,0-15), BIGINT < +/-2^52-1; see note (2)
	xsqlvar:setparts(parts_t)             for SMALLINT, INTEGER, DECIMAL(1-15,0-15), BIGINT < +/-2^52-1
	xsqlvar:getdecimal(df) -> d           for SMALLINT, INTEGER, DECIMAL(1-18,0-18), BIGINT; see note (3)
	xsqlvar:setdecimal(d,sdf)             for SMALLINT, INTEGER, DECIMAL(1-18,0-18), BIGINT; see note (3)
	xsqlvar:getstring() -> s              for VARCHAR, CHAR
	xsqlvar:getstringlength() -> n        for VARCHAR, CHAR
	xsqlvar:setstring(s)                  for VARCHAR, CHAR
	xsqlvar:setpadded(s)                  for VARCHAR, CHAR (adds necessary space padding for CHAR type)
	xsqlvar:getunpadded(s)                for VARCHAR, CHAR (strips any space padding for CHAR type)
	xsqlvar:getblobid() -> blob_id_buf    for BLOB
	xsqlvar:setblobid(blob_id_buf)        for BLOB

	time_meta -> the metatable of time_t objects when representing times
	date_meta -> the metatable of time_t objects when representing dates
	timestamp_meta -> the metatable of time_t objects when representing timestamps
	parts_meta -> the metatable of parts_t objects (for numbers)

	NOTES:
	(1) time_t is as per os.date(), but with the additional field sfrac, meaning fractions of a second,
	an integer in range 0-9999.
	(2) parts_t is {int,frac}
	(3) the df and sdf arguments for getdecimal() and setdecimal() are functions implemented in decimal_*.lua.

	*** XSQLVAR polymorphic get/set ***

	xsqlvar_class.set_handlers -> array of handler functions for set()
	xsqlvar_class.get_handlers -> array of handler functions for get()
	xsqlvar_class.add_set_handler(f)
	xsqlvar_class.add_get_handler(f)

	xsqlvar:set(variant)           for all types: generic setter; see note (4)
	xsqlvar:get() -> variant       for all types: generic getter; see note (5)

	NOTES:
	(4) set() is extendable via public array set_handlers which is an array of setter functions
	which will be called one by one with the parameter recieved by set(), until the first one signals
	success by returning true. you can add setter functions to this array to make it support more data types.
	(5) get() is extendable via public array get_handlers which is an array of getter functions
	which will be called one by one until the first one returns true,some_value. you can add getter
	functions to this array to make it support more data types.

	parameter value type mapping for set(p):

	param type					|	given type		|	setter method			|	where setter is implemented
	----------------------------+-------------------+---------------------------+-----------------------------
	any								nil					setnull()					xsqlvar.lua
	time,date,timestamp				time_t				settime(t)					xsqlvar.lua
	int16,int32,int64; scale = 0	number,string		setnumber(getnumber(n))		xsqlvar.lua
	int16,int32,int64; scale >= 0	parts_t				setparts(parts_t)			xsqlvar.lua
	float,double					number,string		setnumber(getnumber(n))		xsqlvar.lua
	varchar							string				setstring(s)				xsqlvar.lua
	char							string				setpadded(s)				xsqlvar.lua
	int16,int32,int64; any scale	decnumber number	setdecnumber(d)				decimal_ldecnumber.lua
	int16,int32,int64; any scale	bc number			setbc(d)					decimal_lbc.lua
	int16,int32,int64; any scale	mapm number			setmapm(d)					decimal_lmapm.lua
	blob							string				write(s)					blob.lua

	column value type mapping for get():

	column type					|	returned type	|	getter method			|	where getter is implemented
	----------------------------+-------------------+---------------------------+-----------------------------
	any, NULL value					nil					isnull()					xsqlvar.lua
	time,date,timestamp				time_t				gettime()					xsqlvar.lua
	int16,int32,int64; scale = 0	number				getnumber()					xsqlvar.lua
	int16,int32,int64; scale >= 0	parts_t				getparts()					xsqlvar.lua
	float,double					number				getnumber()					xsqlvar.lua
	varchar							string				getstring()					xsqlvar.lua
	char							string				getunpadded()				xsqlvar.lua
	int16,int32,int64; any scale	a bignum number		getbc() or getmapm() etc	decimal_*.lua, whichever loaded first
	blob							strings				segments()					blob.lua

	MORE NOTES:
	- xs:tostring() should work for any value. as an alternative, you can use tostring(xs:get()) which should
	work for all values except nil; since boolean is never returned, you could use tostring(xs:get() or '<NULL>')
	for printing.

	LIMITATIONS:
	- only works on a Lua interpreter with LUA_NUMBER = double.

]=]

module(...,require 'fbclient.init')

xsqlvar_class = {
	set_handlers = {},
	get_handlers = {},
}
xsqlvar_meta = {__index = xsqlvar_class, __type = 'fbclient.xsqlvar'}

function wrap(xs, fbapi, sv, dbh, trh, xs_meta)
	xs.fbapi = fbapi --needed for time and blob funcs
	xs.sv = sv --needed for blob funcs
	xs.dbh = dbh --needed for blob funcs
	xs.trh = trh --needed for blob funcs
	return setmetatable(xs, xs_meta or xsqlvar_meta)
end

function xsqlvar_class:allownull()
	return self.allow_null
end

function xsqlvar_class:isnull()
	return self.allow_null and self.sqlind_buf:get(1,'int')==-1
end

function xsqlvar_class:setnull()
	assert(self.allow_null, 'NULL not allowed')
	self.sqlind_buf:set(1,-1,'int')
end

local sqltypes = {
	SQL_TYPE_TIME='time',		-- TIME
	SQL_TYPE_DATE='date',		-- DATE
	SQL_TIMESTAMP='timestamp',	-- TIMESTAMP
	SQL_SHORT='int16',			-- SMALLINT and DECIMAL(1-4,0-4)
	SQL_LONG='int32',			-- INTEGER and DECIMAL(5-9,0-9)
	SQL_INT64='int64',			-- BIGINT and DECIMAL(10-18,0-18)
	SQL_FLOAT='float',			-- FLOAT
	SQL_DOUBLE='double',		-- DOUBLE PRECISION
	SQL_TEXT='char',			-- CHAR
	SQL_VARYING='varchar',		-- VARCHAR
	SQL_BLOB='blob',			-- BLOB
	SQL_ARRAY='array',			-- ARRAY
}

function xsqlvar_class:type()
	local typ, subtyp = assert(sqltypes[self.sqltype])
	if typ == 'int16' or typ == 'int32' or typ == 'int64' then
		subtyp = 0-self.sqlscale --because in FP 0-0 == +0
	elseif typ == 'varchar' or typ == 'char' then
		subtyp = self.sqllen
	elseif type == 'blob' then
		subtyp = self.subtype
	end
	return typ, subtyp
end

local TM_STRUCT = 'iiiiiiiii' -- C's tm struct: sec,min,hour,day,mon,year,wday,yday,isdst
local ISC_TIME_SECONDS_PRECISION = 10000 -- firebird keeps time in seconds*ISC_TIME_SECONDS_PRECISION.

--BUG: for some reason, in Linux, isc_decode_timestamp writes 2 integers outside the TM_STRUCT space !!
TM_STRUCT = TM_STRUCT..'xxxxxxxx'

local function format_time(t) return os.date('%X', os.time(t))..' '..t.sfrac end
local function format_date(t) return os.date('%x', os.time(t)) end
local function format_timestamp(t) return os.date('%x %X', os.time(t))..' '..t.sfrac end

time_meta = {__tostring = format_time, __type = 'fbclient.time'}
date_meta = {__tostring = format_date, __type = 'fbclient.date'}
timestamp_meta = {__tostring = format_timestamp, __type = 'fbclient.timestamp'}

function xsqlvar_class:gettime(t) -- t is optional so you can reuse a time_t for less garbage in a tight loop.
	assert(not self:isnull(), 'NULL value')
	t = t or {}
	local buf = self.sqldata_buf
	local tm = self.cached_tm_buf
	if not tm then
		tm = alien.buffer(struct.size(TM_STRUCT))
		self.cached_tm_buf = tm -- trick to reuse the tm buffer
	end

	local typ = self.sqltype
	if typ == 'SQL_TYPE_TIME' then
		self.fbapi.isc_decode_sql_time(buf, tm)
		setmetatable(t, time_meta)
		t.sfrac = buf:get(1,'uint') % ISC_TIME_SECONDS_PRECISION
	elseif typ == 'SQL_TYPE_DATE' then
		self.fbapi.isc_decode_sql_date(buf, tm)
		setmetatable(t, date_meta)
	elseif typ == 'SQL_TIMESTAMP' then
		self.fbapi.isc_decode_timestamp(buf, tm)
		setmetatable(t, timestamp_meta)
		t.sfrac = buf:get(1+INT_SIZE,'uint') % ISC_TIME_SECONDS_PRECISION
	else
		asserts(false, 'incompatible data type %s', self:type())
	end

	t.sec,t.min,t.hour,t.day,t.month,t.year,t.wday,t.yday,t.isdst =
		struct.unpack(TM_STRUCT,tm,struct.size(TM_STRUCT))
	t.month = t.month+1
	t.year = t.year+1900
	t.wday = t.wday+1
	t.yday = t.yday+1
	t.isdst = t.isdst ~= 0

	return t
end

function xsqlvar_class:settime(t)
	local buf = self.sqldata_buf
	local tm = struct.pack(TM_STRUCT,t.sec or 0,t.min or 0,t.hour or 0,
									t.day or 1,(t.month or 1)-1,(t.year or 1900)-1900,0,0,0)

	local typ = self.sqltype
	local time_ofs
	if typ == 'SQL_TYPE_TIME' then
		self.fbapi.isc_encode_sql_time(tm, buf)
		time_ofs = 1
	elseif typ == 'SQL_TYPE_DATE' then
		self.fbapi.isc_encode_sql_date(tm, buf)
	elseif typ == 'SQL_TIMESTAMP' then
		self.fbapi.isc_encode_timestamp(tm, buf)
		time_ofs = 1+INT_SIZE
	else
		asserts(false, 'incompatible data type %s', self:type())
	end

	if t.sfrac and time_ofs then
		local sfrac = t.sfrac
		assert(sfrac%1==0, 'arg#1.sfrac integer expected, got float')
		asserts(sfrac >= 0 and sfrac < ISC_TIME_SECONDS_PRECISION,
				'arg#1.sfrac out of range (range is 0 to %d)', ISC_TIME_SECONDS_PRECISION-1)
		if sfrac > 0 then
			buf:set(time_ofs,buf:get(time_ofs,'uint')+sfrac,'uint')
		end
	end

	if self.allow_null then
		self.sqlind_buf:set(1,0,'int')
	end
end

-- this doesn't work on DECIMAL because they can't be accurately represented by floats
function xsqlvar_class:getnumber()
	assert(not self:isnull(), 'NULL value')
	local typ,scale,styp = self.sqltype, self.sqlscale, self:type()
	if typ == 'SQL_FLOAT' then -- FLOAT
		return self.sqldata_buf:get(1,'float')
	elseif typ == 'SQL_DOUBLE' or typ == 'SQL_D_FLOAT' then -- DOUBLE PRECISION
		return self.sqldata_buf:get(1,'double')
	elseif typ == 'SQL_SHORT' then -- SMALLINT
		asserts(scale == 0, 'decimal type %s scale %d (only integers and scale 0 decimals allowed)', styp, scale)
		return self.sqldata_buf:get(1,'short')
	elseif typ == 'SQL_LONG' then -- INTEGER
		asserts(scale == 0, 'decimal type %s scale %d (only integers and scale 0 decimals allowed)', styp, scale)
		return self.sqldata_buf:get(1,'int')
	elseif typ == 'SQL_INT64' then -- BIGINT
		asserts(scale == 0, 'decimal type %s scale %d (only integers and scale 0 decimals allowed)', styp, scale)
		local lo,hi = struct.unpack('Ii', self.sqldata_buf, self.buflen)
		local n = hi*2^32+lo -- overflowing results in +/- INF
		-- we consider it an error to be able to read a number out of the range of setnumber()
		asserts(n >= MIN_LUAINT and n <= MAX_LUAINT, 'number out of range (range is %d to %d)',MIN_LUAINT,MAX_LUAINT)
		return n
	end
	asserts(false, 'incompatible data type %s', styp)
end

-- this doesn't work on DECIMALs because they can't be accurately represented by floats
function xsqlvar_class:setnumber(n)
	local typ,scale,styp = self.sqltype, self.sqlscale, self:type()
	if typ == 'SQL_FLOAT' then
		--TODO: replace this ugly (but safe and efficient, aren't they all) hack to check
		--loss of precision in conversion from double to float
		local oldn = self.sqldata_buf:get(1,'float')
		self.sqldata_buf:set(1,n,'float')
		local p = self.sqldata_buf:get(1,'float')
		if not (n ~= n and p ~= p or n == p) then
			self.sqldata_buf:set(1,oldn,'float')
			assert(false, 'arg#1 number out of precision')
		end
	elseif typ == 'SQL_DOUBLE' or typ == 'SQL_D_FLOAT' then
		self.sqldata_buf:set(1,n,'double')
	elseif typ == 'SQL_SHORT' or typ == 'SQL_LONG' or typ == 'SQL_INT64' then
		asserts(scale == 0, 'decimal type %s scale %d (only integers and scale 0 decimals allowed)', styp, scale)
		assert(n%1==0, 'arg#1 integer expected, got float')
		local range_error = 'arg#1 number out of range (range is %d to %d)'
		if typ == 'SQL_SHORT' then
			asserts(n >= MIN_SHORT and n <= MAX_SHORT, range_error, MIN_SHORT, MAX_SHORT)
			self.sqldata_buf:set(1,n,'short')
		elseif typ == 'SQL_LONG' then
			asserts(n >= MIN_INT and n <= MAX_INT, range_error, MIN_INT, MAX_INT)
			self.sqldata_buf:set(1,n,'int')
		elseif typ == 'SQL_INT64' then
			asserts(n >= MIN_LUAINT and n <= MAX_LUAINT, range_error, MIN_LUAINT, MAX_LUAINT)
			local lo,hi = n%2^32, math.floor(n/2^32)
			self.sqldata_buf:set(1,lo,'uint')
			self.sqldata_buf:set(1+INT_SIZE,hi,'int')
		end
	else
		asserts(false, 'incompatible data type %s', styp)
	end
	if self.allow_null then
		self.sqlind_buf:set(1,0,'int')
	end
end

local decimal_sym = ('%1.1d'):format(3.14):sub(2,2)
local function format_parts(t)
	return ('%d%s%d'):format(t[1],decimal_sym,t[2])
end

parts_meta = {__type = 'fbclient.parts', __tostring = format_parts}

-- this doesn't work with FLOAT or DOUBLE because the fractions can't be accurately represented in floats.
function xsqlvar_class:getparts()
	assert(not self:isnull(), 'NULL value')
	t = t or {}
	local typ, scale = self.sqltype, self.sqlscale
	local n
	if typ == 'SQL_SHORT' then -- SMALLINT
		n = self.sqldata_buf:get(1,'short')
	elseif typ == 'SQL_LONG' then -- INTEGER
		n = self.sqldata_buf:get(1,'int')
	elseif typ == 'SQL_INT64' then -- BIGINT
		local lo,hi = struct.unpack('Ii', self.sqldata_buf, self.buflen)
		n = hi*2^32+lo -- overflowing results in +/- INF
		-- we see it as an error to be able to getparts() a number that is out of the range of setparts()
		asserts(n >= MIN_LUAINT and n <= MAX_LUAINT, 'number out of range (range is %d to %d)', MIN_LUAINT, MAX_LUAINT)
	else
		asserts(false, 'incompatible data type %s', self:type())
	end
	if scale == 0 then
		t[1] = n
		t[2] = 0
	else
		t[1] = math.floor(n*10^scale)
		t[2] = n%10^-scale
	end
	return setmetatable(t, parts_meta)
end

-- this doesn't work with FLOAT or DOUBLE because the fractions can't be accurately represented in floats.
function xsqlvar_class:setparts(t)
	local int,frac = unpack(t)
	assert(int%1==0, 'arg#1[1] integer expected, got float')
	assert(frac%1==0, 'arg#1[2] integer expected, got float')
	local factor = 10^-self.sqlscale
	asserts(frac <= factor-1, 'arg#1[2] out of range (range is %d to %d)', 0, factor-1)
	local n = int*factor+frac
	local typ = self.sqltype
	local range_error = 'arg#1[1] number out of range (range is %d to %d)'
	if typ == 'SQL_SHORT' then
		asserts(n >= MIN_SHORT and n <= MAX_SHORT, range_error, MIN_SHORT, MAX_SHORT)
		self.sqldata_buf:set(1,n,'short')
	elseif typ == 'SQL_LONG' then
		asserts(n >= MIN_INT and n <= MAX_INT, range_error, MIN_INT, MAX_INT)
		self.sqldata_buf:set(1,n,'int')
	elseif typ == 'SQL_INT64' then
		asserts(n >= MIN_LUAINT and n <= MAX_LUAINT, range_error, MIN_LUAINT, MAX_LUAINT)
		local lo,hi = n%2^32, math.floor(n/2^32)
		self.sqldata_buf:set(1,lo,'uint')
		self.sqldata_buf:set(1+INT_SIZE,hi,'int')
	else
		asserts(false, 'incompatible data type %s', self:type())
	end
	if self.allow_null then
		self.sqlind_buf:set(1,0,'int')
	end
end

-- this doesn't work with FLOAT or DOUBLE because the fractions can't be accurately represented in floats.
function xsqlvar_class:getdecimal(df)
	assert(not self:isnull(), 'NULL value')
	local typ = self.sqltype
	if typ == 'SQL_SHORT' then -- SMALLINT or DECIMAL(1-4,0)
		return df(self.sqldata_buf:get(1,'short'), 0, self.sqlscale)
	elseif typ == 'SQL_LONG' then -- INTEGER or DECIMAL(5-9,0)
		return df(self.sqldata_buf:get(1,'int'), 0, self.sqlscale)
	elseif typ == 'SQL_INT64' then -- BIGINT or DECIMAL(10-18,0)
		local lo,hi = struct.unpack('Ii', self.sqldata_buf, self.buflen)
		return df(lo, hi, self.sqlscale)
	end
	asserts(false, 'incompatible data type %s', self:type())
end

-- this doesn't work with FLOAT or DOUBLE because the fractions can't be accurately represented in floats.
function xsqlvar_class:setdecimal(d,sdf)
	local typ = self.sqltype
	local lo,hi
	local range_error = 'arg#1 number out of range (range is %d to %d)'
	if typ == 'SQL_SHORT' then
		asserts(lo >= MIN_SHORT and lo <= MAX_SHORT, range_error, MIN_SHORT, MAX_SHORT)
		lo,hi = sdf(d, self.sqlscale)
		self.sqldata_buf:set(1,lo,'short')
	elseif typ == 'SQL_LONG' then
		asserts(lo >= MIN_INT and lo <= MAX_INT, range_error, MIN_INT, MAX_INT)
		lo,hi = sdf(d, self.sqlscale)
		self.sqldata_buf:set(1,lo,'int')
	elseif typ == 'SQL_INT64' then
		lo,hi = sdf(d, self.sqlscale)
		assert(lo >= 0 and lo <= MAX_UINT and hi >= MIN_INT and hi <= MAX_INT, 'arg#1 number out of range')
		self.sqldata_buf:set(1,lo,'uint')
		self.sqldata_buf:set(1+INT_SIZE,hi,'int')
	else
		asserts(false, 'incompatible data type %s', self:type())
	end
	if self.allow_null then
		self.sqlind_buf:set(1,0,'int')
	end
end

function xsqlvar_class:getstring()
	assert(not self:isnull(), 'NULL value')
	local typ = self.sqltype
	if typ == 'SQL_TEXT' then
		return self.sqldata_buf:tostring(self.sqllen) -- CHAR type, space padded
	elseif typ == 'SQL_VARYING' then
		return struct.unpack('hc0', self.sqldata_buf, self.buflen) -- VARCHAR type
	else
		asserts(false, 'incompatible data type %s',self:type())
	end
end

function xsqlvar_class:getstringlength()
	assert(not self:isnull(), 'NULL value')
	local typ = self.sqltype
	if typ == 'SQL_TEXT' then
		return self.sqllen
	elseif typ == 'SQL_VARYING' then
		return struct.unpack('h', self.sqldata_buf, self.buflen)
	else
		asserts(false, 'incompatible data type %s',self:type())
	end
end

function xsqlvar_class:setstring(s)
	local typ = self.sqltype
	if typ == 'SQL_TEXT' then
		asserts(#s == self.sqllen, 'expected string of exactly %d bytes', self.sqllen)
		alien.memcpy(self.sqldata_buf, s)
	elseif typ == 'SQL_VARYING' then
		local buf = self.sqldata_buf
		asserts(#s <= self.sqllen, 'expected string of max. %d bytes', self.sqllen)
		buf:set(1,#s,'short')
		alien.memcpy(buf:topointer(1+SHORT_SIZE), s)
	else
		asserts(false, 'incompatible data type %s',self:type())
	end
	if self.allow_null then
		self.sqlind_buf:set(1,0,'int')
	end
end

function xsqlvar_class:setpadded(s)
	if self.sqltype == 'SQL_TEXT' then
		self:setstring(s..(' '):rep(select(2,self:type())-#s))
	else
		self:setstring(s)
	end
end

function xsqlvar_class:getunpadded()
	if self.sqltype == 'SQL_TEXT' then
		return (self:getstring():gsub(' *$',''))
	else
		self:getstring()
	end
end

function xsqlvar_class:getblobid()
	--we make an exception and give the null blobid buffer even if the record is null
	asserts(self.sqltype == 'SQL_BLOB', 'incompatible data type %s', self:type())
	return self.sqldata_buf
end

function xsqlvar_class:setblobid(blob_id_buf)
	asserts(self.sqltype == 'SQL_BLOB', 'incompatible data type %s', self:type())
	if self.sqldata_buf ~= blob_id_buf then -- this could be a newly created blob_id_buf
		alien.memcpy(self.sqldata_buf, blob_id_buf, self.buflen)
	end
	if self.allow_null then
		self.sqlind_buf:set(1,0,'int')
	end
end

function xsqlvar_class:set(p)
	for _,f in ipairs(self.set_handlers) do
		if f(self,p,self:type()) then
			return
		end
	end
	asserts(false, 'set(%s) not implemented for type %s (%s)',type(p),self:type())
end

function xsqlvar_class:get()
	for _,f in ipairs(self.get_handlers) do
		local ok,x = f(self,self:type())
		if ok then
			return x
		end
	end
	asserts(false, 'get() not implemented for type %s (%s)',self:type())
end

function xsqlvar_class.add_get_handler(f)
	xsqlvar_class.get_handlers[#xsqlvar_class.get_handlers+1] = f
end

function xsqlvar_class.add_set_handler(f)
	xsqlvar_class.set_handlers[#xsqlvar_class.set_handlers+1] = f
end

local add_get_handler = xsqlvar_class.add_get_handler
local add_set_handler = xsqlvar_class.add_set_handler

add_set_handler(
	function(self,p,typ,opt) --nil -> NULL
		if p == nil then
			self:setnull(p)
			return true
		end
	end
)

add_set_handler(
	function(self,p,typ,opt) --indexable for time|date|timestamp -> settime()
		if (typ == 'time' or typ == 'date' or typ == 'timestamp')
			and applicable(p,'__index')
		then
			self:settime(p)
			return true
		end
	end
)

add_set_handler(
	function(self,p,typ,opt) --indexable for all integers -> setparts({int,frac})
		if (typ == 'int16' or typ == 'int32' or typ == 'int64')
			and applicable(p,'__index')
		then
			self:setparts(p)
			return true
		end
	end
)

add_set_handler(
	function(self,p,typ,opt) --number for all integers w/scale=0 -> setnumber()
		if (typ == 'float' or typ == 'double' or
				((typ == 'int16' or typ == 'int32' or typ == 'int64') and opt == 0))
			and type(p) == 'number'
		then  -- no auto-coercion for numbers
			self:setnumber(p)
			return true
		end
	end
)

add_set_handler(
	function(self,p,typ,opt) --string for varchars
		if typ == 'varchar' and type(p) == 'string' then -- no auto-coercion for strings
			self:setstring(p)
			return true
		end
	end
)

add_set_handler(
	function(self,p,typ,opt) --string for chars
		if typ == 'char' and type(p) == 'string' then -- no auto-coercion for strings
			self:setpadded(p)
			return true
		end
	end
)

add_get_handler(
	function(self,typ,opt)
		if self:isnull() then
			return true,nil
		end
	end
)

add_get_handler(
	function(self,typ,opt)
		if typ == 'time' or typ == 'date' or typ == 'timestamp' then
			return true,self:gettime()
		end
	end
)

add_get_handler(
	function(self,typ,opt)
		if typ == 'float' or typ == 'double' or
			((typ == 'int16' or typ == 'int32') and opt == 0) then
			return true,self:getnumber()
		end
	end
)

add_get_handler(
	function(self,typ,opt)
		if (typ == 'int16' or typ == 'int32') and opt ~= 0 then
			return true,self:getparts()
		end
	end
)

add_get_handler(
	function(self,typ,opt)
		if typ == 'varchar' then
			return true,self:getstring()
		end
	end
)

add_get_handler(
	function(self,typ,opt)
		if typ == 'char' then
			return true,self:getunpadded()
		end
	end
)


