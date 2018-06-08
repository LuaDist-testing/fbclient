--[[
	utility functions.
	they get registered in the inherited environment of all modules (see package.lua).

	version() -> major,minor,build
	index(t) -> new_t
	keys(t) -> a
	deep_copy(t,[target={}]) -> target
	count(t,[upto=math.huge]) -> n
	asserts(v,s,...)
	xtype(x) -> uses x's metatable __type field to get the type of x, or falls back to type()
	applicable(x,metamethod_name) -> tells if a certain operation is applicable to an object
	xunpack(t,i,j) -> unpack() defined in terms of __index and __len for non-tables
	isbuffer(b) -> true|false; true if b is a buffer created with alien.buffer()
	dump(v)

	INT_SIZE, SHORT_SIZE, DOUBLE_SIZE, POINTER_SIZE, MIN_INT, MAX_INT, MAX_UINT
	MIN_SHORT, MAX_SHORT, MAX_USHORT, MAX_BYTE, MIN_SCHAR, MAX_SCHAR, MIN_LUAINT, MAX_LUAINT

	isinteger(v) -> true|false
	isint(v) -> true|false
	isuint(v) -> true|false
	isshort(v) -> true|false
	isushort(v) -> true|false
	isbyte(v) -> true|false
	isschar(v) -> true|false

]]

local pairs = pairs
local getmetatable = getmetatable
local setmetatable = setmetatable
local math = math
local next = next
local assert = assert
local type = type
local unpack = unpack
local tostring = tostring
local print = print
local select = select
local alien = require 'alien'

module(...) --use of require'fbclient.init' on this module would create a circular dependency

function version()
	return 0,4,0
end

local ALIEN_BUFFER_META = getmetatable(alien.buffer(1))

--turn values into keys
function index(t)
	newt={}
	for k,v in pairs(t) do
		newt[v]=k
	end
	return newt
end

--return an array of keys of t
function keys(t)
	newt={}
	for k,v in pairs(t) do
		newt[#newt+1]=k
	end
	return newt
end

--simple deep copy function without cycle detection.
--uses assignment to copy objects (except tables), so userdata and thread types are not supported.
--the metatable is not copied, just referenced, except if it's the source object itself, when it's reassigned.
function deep_copy(t,target)
	if not t then return target end
	target = target or {}
	for k,v in pairs(t) do
		target[k] = applicable(v,'__pairs') and deep_copy(v,target[k]) or v
	end
	local mt = getmetatable(t)
	return setmetatable(target, mt == t and target or mt)
end

--count the elements in t (optionally upto some number)
function count(t,upto)
	upto = upto or math.huge
	local i,k = 0,next(t)
	while k and i < upto do
		i = i+1
		k = next(t,k)
	end
	return i
end

--garbageless assert with string formatting
function asserts(v,s,...)
	if v then return v,s,... end
	assert(false, s:format(select(1,...)))
end

function xtype(x)
	local mt = getmetatable(x)
	return mt and mt.__type or type(x)
end

local applicable_prims = {
	__call = 'function',
	__index = 'table', __newindex = 'table', __mode = 'table',
	__pairs = 'table', __ipairs = 'table', __len = 'table',
	__tonumber = 'number',
		__add = 'number', __sub = 'number', __mul = 'number', __div = 'number', __mod = 'number',
		__pow = 'number', __unm = 'number',
	__tostring = 'string', __concat = 'string',
}

function applicable(x,mm)
	if applicable_prims[mm] == type(x) then
		return true
	else
		local mt = getmetatable(x)
		return mt and mt[mm] or false
	end
end

function xunpack(t,i,j)
	if type(t) == 'table' then
		return unpack(t,i,j)
	else
		i = i or 1
		j = j or #t
		if i <= j then
			return t[i], xunpack(t,i+1,j) --not a tail call :(
		end
	end
end

function isbuffer(b)
	return getmetatable(b) == ALIEN_BUFFER_META
end

local function dump_recursive(v,k,i)
	i = i or 0
	if applicable(v,'__pairs') then
		print((' '):rep(i)..(k and '['..tostring(k)..'] => ' or '')..type(v))
		for kk,vv in pairs(v) do
			dump_recursive(vv,kk,i+2)
		end
	else
		print((' '):rep(i)..(k and '['..k..'] => ' or '')..tostring(v)..', type '..type(v))
	end
end

-- table dump for debugging purposes
function dump(v) dump_recursive(v) end

-- basic stuff for working with binary integers
INT_SIZE	= alien.sizeof('int')
SHORT_SIZE	= alien.sizeof('short')
DOUBLE_SIZE = alien.sizeof('double')
POINTER_SIZE= alien.sizeof('pointer')
MIN_INT		= -2^31
MAX_INT		= 2^31-1
MAX_UINT	= 2^32-1
MIN_SHORT	= -2^15
MAX_SHORT	= 2^15-1
MAX_USHORT	= 2^16-1
MAX_BYTE	= 2^8-1
MIN_SCHAR	= -2^7
MAX_SCHAR	= 2^7-1
MIN_LUAINT	= -2^52
MAX_LUAINT	= 2^52-1

function isinteger(v) return v%1 == 0 end
function isint(v) return v%1 == 0 and v >= MIN_INT and v <= MAX_INT end
function isuint(v) return v%1 == 0 and v >= 0 and v <= MAX_UINT end
function isshort(v) return v%1 == 0 and v >= MIN_SHORT and v <= MAX_SHORT end
function isushort(v) return v%1 == 0 and v >= 0 and v <= MAX_USHORT end
function isbyte(v) return v%1 == 0 and v >= 0 and v <= MAX_BYTE end
function isschar(v) return v%1 == 0 and v >= MIN_SCHAR and v <= MAX_SCHAR end

