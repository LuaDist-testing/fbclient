
object_class = setmetatable({},{__type = 'object')

function object_class:type()
	return getmetatable(self).__type
end

function object_class:parent()
	return getmetatable(self).__index
end

function object_class:callp(method,...)
	return self:parent()[method](self,...)
end

function object_class:new()
	return setmetatable({},getmetatable(self))
end

function object_class:derive(type)
	return setmetatable({},{__index = self, __type = type})
end

function object_class:copy()
	local t = setmetatable({},getmetatable(self))
	for k,v in pairs(self) do
		t[k] = v
	end
	return t
end

local function virtual_table(obj, getter_func, setter_func, counter_func)
	local t = newproxy(true)
	local mt = getmetatable(t)
	mt.__index = getter_func and function(t,k) return getter_func(obj,k) end
	mt.__newindex = setter_func and function(t,k,v) setter_func(obj,k,v) end
	mt.__len = counter_func and function(t) return counter_func(obj) end
	return t
end

database_class = object_class:derive('database')

function database_class:new(attachment)
	local t = self:callp('new')
	t.attachment = attachment
	t.tables = virtual_table(t, t.get_table, nil, t.table_count)
	t.domains = virtual_table(t, t.get_domain, nil, t.domain_count)
	return t
end

table_class = object_class:derive('table')

function table_class:new(database, table_name)
	local t = self:callp('new')
	t.database = database
	t.name = table_name
	return t
end

function database_class:get_table(table_name)
	return table_class:new(self, table_name)
end

function database_class:table_count() end

function database_class:get_domain(i_or_name) end
function database_class:domain_count() end

function database_class:get_generator(i_or_name) end
function database_class:generator_count() end

function database_class:get_exception(i_or_name) end
function database_class:exception_count() end

function database_class:get_foreign_key(i_or_name) end
function database_class:foreign_key_count() end

function database_class:get_index(i_or_name) end
function database_class:set_index(i_or_name,opts) end
function database_class:index_count() end

function database_class:get_trigger(i_or_name) end
function database_class:set_trigger(i_or_name,opts) end
function database_class:trigger_count() end

function database_class:get_procedure(i_or_name) end
function database_class:set_procedure(i_or_name,opts) end
function database_class:procedure_count() end

function database_class:get_view(i_or_name) end
function database_class:set_view(i_or_name,opts) end
function database_class:view_count() end

function table_class:get_column(i_or_name)
	local t = setmetatable({}, column_metatable)
	t.name = name
	t.table = self
	t.domain = nil
	t.type = nil
	t.scale = nil
	t.precision = nil
	t.length = nil
	t.subtype = nil
	t.not_null = nil
	return t
end

function table_class:column_count() end

function table_class:get_pk_columns(i) end
function table_class:pk_column_count() end

function table_class:get_check_expressions(i_or_name) end
function table_class:set_check_expressions(i_or_name,code) end
function table_class:check_expression_count() end

function table_class:get_foreign_key(i_or_name) end
function table_class:foreign_key_count() end


