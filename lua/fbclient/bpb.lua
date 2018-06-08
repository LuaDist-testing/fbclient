--[=[
	BPB (Blob Parameter Block) structure: for using blob filters when reading/writing blobs.

	encode(bpb_options_t) -> BPB encoded string.

	USAGE: pass it to isc_open_blob2() or isc_create_blob2() to open/create a filtered blob object.

]=]

module(...,require 'fbclient.init')

local pb = require 'fbclient.pb'

local codes = {
	isc_bpb_source_type               = 1, --signed byte: subtype at application endpoint; mandatory
	isc_bpb_target_type               = 2, --signed byte: subtype at database endpoint; mandatory
	isc_bpb_type                      = 3, --blob type: isc_bpb_type_segmented or stream, default is segmented
	isc_bpb_source_interp             = 4, --source charset; default = ?
	isc_bpb_target_interp             = 5, --target charset; default = ?
	isc_bpb_filter_parameter          = 6, --filters convert one blob type to another. the parameter could
										   --hold an encrypt/decrypt key. since there's no SQL language support
										   --for blob filtering, the question is moot.
	isc_bpb_storage                   = 7, --blob storage: optional, isc_bpb_storage_main or temp, default is main
}

local isc_bpb_type_enum = {
	isc_bpb_type_segmented = 0,
	isc_bpb_type_stream = 1,
}

local isc_bpb_storage_enum = {
	isc_bpb_storage_main = 0,
	isc_bpb_storage_temp = 2,
}

local encoders = {
	isc_bpb_source_type		= pb.encode_schar,
	isc_bpb_target_type		= pb.encode_schar,
	isc_bpb_type			= pb.encode_enum(isc_bpb_type_enum),
	isc_bpb_source_interp	= pb.encode_short,
	isc_bpb_target_interp	= pb.encode_short,
	isc_bpb_filter_parameter= pb.encode_short,
	isc_bpb_storage			= pb.encode_enum(isc_bpb_storage_enum),
}

function encode(opts)
	return pb.encode('BPB', '\1', opts, codes, encoders)
end

