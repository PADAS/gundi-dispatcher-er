from types import SimpleNamespace

from core import batch_progress


def _items(*gundi_ids):
    return [SimpleNamespace(gundi_id=g) for g in gundi_ids]


def test_progress_key_hashes_provider_key_and_is_delimiter_safe():
    key = batch_progress.progress_key(
        "8a5535df-1b9b-412b-9fd5-e29b09582222",
        "338225f3-91f9-4fe1-b013-353a229ce504",
        "gundi.movebank:abc123",  # contains BOTH separators
    )
    assert key.startswith("batch_progress.8a5535df-1b9b-412b-9fd5-e29b09582222.")
    # 4 segments exactly: prefix, batch_id, destination_id, provider digest
    assert len(key.split(".")) == 4
    assert "movebank" not in key  # provider_key never appears literally


def test_progress_key_differs_per_provider_key():
    args = ("batch-1", "dest-1")
    assert batch_progress.progress_key(*args, "key-a") != batch_progress.progress_key(*args, "key-b")


def test_fingerprint_is_stable_and_order_sensitive():
    a = batch_progress.fingerprint(_items("id-0", "id-1", "id-2"))
    assert a == batch_progress.fingerprint(_items("id-0", "id-1", "id-2"))
    assert len(a) == 8
    assert a != batch_progress.fingerprint(_items("id-2", "id-1", "id-0"))  # reorder
    assert a != batch_progress.fingerprint(_items("id-0", "id-1"))          # removed
    assert a != batch_progress.fingerprint(_items("id-0", "id-1", "id-2", "id-3"))  # added


def test_encode_sets_expected_bits():
    fp = b"\x00" * 8
    assert batch_progress.encode(fp, {0, 1, 2}, 3)[8:] == bytes([0b00000111])
    assert batch_progress.encode(fp, {0, 2}, 3)[8:] == bytes([0b00000101])
    assert batch_progress.encode(fp, {8}, 9)[8:] == bytes([0b00000000, 0b00000001])


def test_encode_ignores_out_of_range_indices():
    fp = b"\x00" * 8
    assert batch_progress.encode(fp, {-1, 5, 99}, 3)[8:] == bytes([0b00000000])


def test_encode_decode_round_trips_across_byte_boundaries():
    for n in (0, 1, 7, 8, 9, 200):
        items = _items(*[f"id-{i}" for i in range(n)])
        fp = batch_progress.fingerprint(items)
        delivered = set(range(0, n, 3))
        raw = batch_progress.encode(fp, delivered, n)
        assert batch_progress.decode(raw, fp, n) == delivered


def test_decode_returns_empty_when_record_missing_or_truncated():
    fp = b"\x01" * 8
    assert batch_progress.decode(None, fp, 3) == set()
    assert batch_progress.decode(b"", fp, 3) == set()
    assert batch_progress.decode(b"\x01" * 7, fp, 3) == set()  # shorter than fingerprint


def test_decode_returns_empty_on_fingerprint_mismatch():
    # The envelope was re-published with a different item list, so positional
    # bits no longer refer to the same observations. Must fail open.
    items = _items("id-0", "id-1", "id-2")
    raw = batch_progress.encode(batch_progress.fingerprint(items), {0, 1, 2}, 3)
    other = batch_progress.fingerprint(_items("id-9", "id-8", "id-7"))
    assert batch_progress.decode(raw, other, 3) == set()


def test_decode_treats_short_bitmap_as_absent_bits():
    fp = b"\x02" * 8
    raw = fp + bytes([0b00000101])  # only 1 byte of bitmap, but n spans 2
    assert batch_progress.decode(raw, fp, 12) == {0, 2}


def test_decode_ignores_extra_bitmap_bytes_beyond_n():
    fp = b"\x03" * 8
    raw = fp + bytes([0b11111111, 0b11111111])
    assert batch_progress.decode(raw, fp, 3) == {0, 1, 2}


def test_decode_returns_empty_for_non_bytes_value():
    # A cache returning a str (or anything unexpected) must fail open, not raise.
    fp = b"\x04" * 8
    assert batch_progress.decode("not-bytes", fp, 3) == set()
