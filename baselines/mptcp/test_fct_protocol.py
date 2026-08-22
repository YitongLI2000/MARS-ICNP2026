import socket
import unittest

import fct_protocol
import receiver


class FctProtocolTest(unittest.TestCase):
    def test_standard_geometry_matches_mars_reference(self):
        self.assertEqual(fct_protocol.MARS_EXPECTED_MESSAGES, 3032)
        self.assertEqual(fct_protocol.MARS_MESSAGE_SIZE_BYTES, 6016)
        self.assertEqual(fct_protocol.MARS_EXPECTED_BYTES, 18240512)

    def test_start_preamble_round_trip(self):
        timestamp = 123456789012345
        encoded = fct_protocol.encode_start_preamble(timestamp)

        self.assertEqual(len(encoded), fct_protocol.START_PREAMBLE_SIZE)
        self.assertEqual(fct_protocol.decode_start_preamble(encoded), timestamp)

    def test_receiver_reads_a_fragmented_start_preamble(self):
        sender_socket, receiver_socket = socket.socketpair()
        self.addCleanup(sender_socket.close)
        self.addCleanup(receiver_socket.close)
        encoded = fct_protocol.encode_start_preamble(987654321)
        sender_socket.sendall(encoded[:3])
        sender_socket.sendall(encoded[3:])

        self.assertEqual(receiver.receive_start_preamble(receiver_socket), 987654321)

    def test_start_preamble_is_not_counted_as_application_payload(self):
        sender_socket, receiver_socket = socket.socketpair()
        self.addCleanup(sender_socket.close)
        self.addCleanup(receiver_socket.close)
        payload = b"application-payload"
        sender_socket.sendall(fct_protocol.encode_start_preamble(123) + payload)

        self.assertEqual(receiver.receive_start_preamble(receiver_socket), 123)
        self.assertEqual(receiver_socket.recv(len(payload)), payload)

    def test_invalid_start_preamble_is_rejected(self):
        encoded = bytearray(fct_protocol.encode_start_preamble(123))
        encoded[0] ^= 0xFF

        with self.assertRaisesRegex(ValueError, "magic"):
            fct_protocol.decode_start_preamble(bytes(encoded))


if __name__ == "__main__":
    unittest.main()
