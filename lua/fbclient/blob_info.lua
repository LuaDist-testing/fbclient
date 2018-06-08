--[[
	BLOB_INFO: request info about a blob.
	this is the aux. library to encode the request buffer and decode the reply buffer.

	encode(options_t) -> encoded options string
	decode(info_buf, info_buf_len) -> decoded info table

	USAGE:
	- use it with isc_blob_info() to encode the info request and decode the info result.
	- see info_codes table below for what you can request and the structure and meaning of the results.

]]

module(...,require 'fbclient.init')

local info = require 'fbclient.info'

local info_codes = {
	isc_info_blob_num_segments	= 4, --total number of segments
	isc_info_blob_max_segment	= 5, --length of the longest segment
	isc_info_blob_total_length	= 6, --total size, in bytes, of blob
	isc_info_blob_type			= 7, --type of blob (0: segmented, or 1: stream)
}

local info_code_lookup = index(info_codes)

local info_buf_sizes = {
	isc_info_blob_num_segments	= INT_SIZE,	-- could not test (returns data_not_ready)
	isc_info_blob_max_segment	= INT_SIZE,
	isc_info_blob_total_length	= INT_SIZE,
	isc_info_blob_type			= 1,
}

local isc_bpb_type_enum = {
	isc_bpb_type_segmented = 0,
	isc_bpb_type_stream = 1,
}

local decoders = {
	isc_info_blob_num_segments	= info.decode_unsigned,
	isc_info_blob_max_segment	= info.decode_unsigned,
	isc_info_blob_total_length	= info.decode_unsigned,
	isc_info_blob_type			= info.decode_enum(isc_bpb_type_enum),
}

function encode(opts)
	return info.encode('BLOB_INFO', opts, info_codes, info_buf_sizes)
end

function decode(info_buf, info_buf_len)
	return info.decode('BLOB_INFO', info_buf, info_buf_len, info_code_lookup, decoders)
end

