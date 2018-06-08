--[=[
	lbc binding for decimal numbers.

	df(lo,hi,scale) -> d; to be used with getdecimal()
	sdf(d,scale)	-> lo,hi; to be used with setdecimal()
	isbc(x)			-> true|false; the bc library should provide this but since it doesn't...

	xsqlvar:getbc() -> d
	xsqlvar:setbc(d)

	xsqlvar:set(d), extended to support bc-type decimals
	xsqlvar:get() -> d, extended to support bc-type decimals

	USAGE: just require this module if you have lbc installed. don't forget to initialize
	the bc library first by calling bc.digits(n), where n = max. number of decimals you will
	ever use (18 is a safe minimum if you'll only use bc with firebird).

	LIMITATIONS:
	- assumes 2's complement signed int64 format (no byte order assumption though).
	- only tested on linux.

]=]

module(...,require 'fbclient.init')

local bc = require 'bc' -- yes, the module is called bc, not lbc!
local xsqlvar_class = require('fbclient.xsqlvar').xsqlvar_class
local BC_ZERO = bc.number(0)
local BC_META = getmetatable(BC_ZERO)

-- convert the lo,hi dword pairs of a 64bit integer into a decimal number and scale it down.
function df(lo,hi,scale)
	return (bc.number(hi)*2^32+lo)*10^scale
end

-- scale up a decimal number and convert it into the corresponding lo,hi dword pairs of its int64 representation.
function sdf(d,scale)
	local hi,lo = bc.divmod(d*10^-scale,2^32) --divmod returns quotient,reminder.
	if d < BC_ZERO then
		hi = hi-1
		lo = lo + 2^32
	end
	return bc.tonumber(lo), bc.tonumber(hi)
end

function xsqlvar_class:getbc()
	return self:getdecimal(df)
end

function xsqlvar_class:setbc(d)
	self:setdecimal(d,sdf)
end

function isbc(x)
	return getmetatable(x) == BC_META
end

xsqlvar_class.add_set_handler(
	function(self,p,typ,opt)
		if isbc(p) and (typ == 'int16' or typ == 'int32' or typ == 'int64') then
			self:setbc(p)
			return true
		end
	end
)

xsqlvar_class.add_get_handler(
	function(self,typ,opt)
		if typ == 'int16' or typ == 'int32' or typ == 'int64' then
			return true,self:getbc()
		end
	end
)