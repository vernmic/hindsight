from conftest import FakeClient


def test_legacy_count_cap_and_uncapped_mode(provider):
    for cap, count in [(2, 2), (0, 3)]:
        instance, fake = provider(
            {"recall_sync": True, "recall_max_results": cap},
            FakeClient(recall_texts=["first-fact", "second-fact", "third-fact"]),
        )
        text = instance.prefetch("current task")
        assert "first-fact" in text and "second-fact" in text
        assert ("third-fact" in text) is (count == 3)
        instance.shutdown()


def test_saved_bridge_is_snapshotted_and_cleared_on_session_switch(provider):
    instance, fake = provider({"bank_id": "hermes-ops"})
    instance._previous_retained_turn = "Nimbus option: three parcel comparisons."
    instance._session_turns = ["This option remains deferred."]
    job = instance._make_turn_retain_job(
        list(instance._session_turns), document_id="old", update_mode="append", label="test", track_ops=False
    )
    instance._previous_retained_turn = "later text"
    job()
    assert "Nimbus option" in fake.retains[-1]["items"][0]["context"]
    instance._session_turns = []
    instance.on_session_switch("next-session")
    instance._session_turns = ["An unrelated new claim."]
    instance._make_turn_retain_job(
        list(instance._session_turns), document_id="new", update_mode="append", label="test", track_ops=False
    )()
    assert "Nimbus option" not in fake.retains[-1]["items"][0]["context"]
    assert "later text" not in fake.retains[-1]["items"][0]["context"]
    instance.shutdown()
