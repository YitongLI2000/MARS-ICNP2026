import struct


MARS_FCT_STANDARD = "mars_3032x6016_v1"
MARS_EXPECTED_MESSAGES = 3032
MARS_MESSAGE_SIZE_BYTES = 6016
MARS_EXPECTED_BYTES = MARS_EXPECTED_MESSAGES * MARS_MESSAGE_SIZE_BYTES
MARS_FCT_METRIC = "logical_chunk_equivalent_byte_completion"
MARS_START_BOUNDARY = "sender_before_start_preamble"
MARS_LOGICAL_UNIT = "application_chunk"

START_PREAMBLE_MAGIC = b"MPTFCT01"
START_PREAMBLE_STRUCT = struct.Struct("!8sQ")
START_PREAMBLE_SIZE = START_PREAMBLE_STRUCT.size


def encode_start_preamble(monotonic_ns):
    if not isinstance(monotonic_ns, int) or not 0 < monotonic_ns < 2**64:
        raise ValueError("monotonic_ns must be a positive unsigned 64-bit integer")
    return START_PREAMBLE_STRUCT.pack(START_PREAMBLE_MAGIC, monotonic_ns)


def decode_start_preamble(data):
    if len(data) != START_PREAMBLE_SIZE:
        raise ValueError(
            f"start preamble must be exactly {START_PREAMBLE_SIZE} bytes"
        )
    magic, monotonic_ns = START_PREAMBLE_STRUCT.unpack(data)
    if magic != START_PREAMBLE_MAGIC:
        raise ValueError("invalid start preamble magic")
    if monotonic_ns <= 0:
        raise ValueError("invalid start preamble monotonic timestamp")
    return monotonic_ns
