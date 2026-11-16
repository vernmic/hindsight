-- One read-only public row with an embedding.
--
-- The recorded-page writer suite copies an embedding out of a live row to prove
-- the round trip survives apply and rollback. A fixture database needs such a
-- row to exist; the suite only ever reads it into a TEMP table, so this row is
-- never mutated. Its identity is fixed so the run can assert the public table
-- still holds exactly this content afterwards.

INSERT INTO memory_units (id, bank_id, text, fact_type, embedding, tags)
VALUES (
    '00000000-0000-0000-0000-00000000e001',
    'hermes-ops',
    'seeded fixture row carrying an embedding',
    'world',
    (SELECT array_fill(0.5::real, ARRAY[384])::vector),
    '{kind:fact,scope:hermes-ops}'
)
ON CONFLICT (id) DO NOTHING;
