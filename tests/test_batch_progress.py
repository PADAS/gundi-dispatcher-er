import hashlib
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


def test_fingerprint_does_not_collide_across_item_boundaries():
    # gundi_id is Union[UUID, str] in gundi_core, so a non-UUID string
    # containing "|" is schema-legal. A delimiter join would make these two
    # DIFFERENT item lists hash identically ("a|b" + "c" == "a" + "b|c" once
    # joined with "|"), which would let decode() report item 0 of B ("a") as
    # delivered because it matches A's fingerprint - a false match, which
    # skips a never-delivered observation instead of failing open.
    a = _items("a|b", "c")
    b = _items("a", "b|c")
    fp_a = batch_progress.fingerprint(a)
    fp_b = batch_progress.fingerprint(b)
    assert fp_a != fp_b

    raw = batch_progress.encode(fp_a, {0}, 2)
    # Decoding A's record against B's fingerprint must fail open (empty set),
    # never silently report a match against the wrong item list.
    assert batch_progress.decode(raw, fp_b, 2) == set()


def test_fingerprint_count_prefix_is_fixed_width():
    # The leading item count is a 4-byte big-endian integer, not decimal text.
    # A decimal count is variable-length, which would leave injectivity resting
    # on an argument about the largest possible single gundi_id rather than on
    # the encoding itself. Pinning the exact digest keeps that from silently
    # regressing to str(len(items)).
    expected = hashlib.sha256()
    expected.update((1).to_bytes(4, "big"))
    raw_id = b"id-0"
    expected.update(len(raw_id).to_bytes(4, "big"))
    expected.update(raw_id)
    assert batch_progress.fingerprint(_items("id-0")) == expected.digest()[:8]


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


def test_decode_returns_empty_when_expected_fingerprint_has_wrong_length():
    # decode() must not trust a caller-supplied fingerprint of the wrong
    # length - it can never legitimately match a validly-encoded record, so
    # comparing against it anyway would just be inviting a spurious match.
    items = _items("id-0", "id-1", "id-2")
    raw = batch_progress.encode(batch_progress.fingerprint(items), {0, 1, 2}, 3)
    assert batch_progress.decode(raw, b"\x00" * 7, 3) == set()  # too short
    assert batch_progress.decode(raw, b"\x00" * 9, 3) == set()  # too long


def test_read_progress_returns_cached_value(mocker):
    db = mocker.MagicMock()
    db.get.return_value = b"payload"
    mocker.patch("core.utils._cache_db", db)

    assert batch_progress.read_progress("b1", "d1", "pk") == b"payload"
    db.get.assert_called_once_with(batch_progress.progress_key("b1", "d1", "pk"))


def test_read_progress_returns_none_on_redis_error(mocker):
    db = mocker.MagicMock()
    db.get.side_effect = RuntimeError("redis down")
    mocker.patch("core.utils._cache_db", db)

    assert batch_progress.read_progress("b1", "d1", "pk") is None


def test_write_progress_stores_encoded_record_with_ttl(mocker):
    db = mocker.MagicMock()
    mocker.patch("core.utils._cache_db", db)
    fp = batch_progress.fingerprint(_items("id-0", "id-1", "id-2"))

    batch_progress.write_progress("b1", "d1", "pk", fp, {0, 2}, 3, ttl=90000)

    db.setex.assert_called_once_with(
        name=batch_progress.progress_key("b1", "d1", "pk"),
        time=90000,
        value=fp + bytes([0b00000101]),
    )


def test_write_progress_is_a_noop_when_nothing_delivered(mocker):
    # An all-zero record is indistinguishable from a missing one, so never
    # write it - that keeps decode's empty-set result unambiguous.
    db = mocker.MagicMock()
    mocker.patch("core.utils._cache_db", db)

    batch_progress.write_progress("b1", "d1", "pk", b"\x00" * 8, set(), 3, ttl=90000)

    assert not db.setex.called


def test_write_progress_swallows_redis_error(mocker):
    db = mocker.MagicMock()
    db.setex.side_effect = RuntimeError("redis down")
    mocker.patch("core.utils._cache_db", db)

    batch_progress.write_progress("b1", "d1", "pk", b"\x00" * 8, {0}, 3, ttl=90000)  # must not raise
