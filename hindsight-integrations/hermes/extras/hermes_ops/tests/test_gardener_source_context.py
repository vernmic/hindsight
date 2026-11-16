import unittest
from gardener_source_context import select_document


class ContextTests(unittest.TestCase):
    def test_complete_bridge_paragraph_and_code(self):
        padding = "unrelated paragraph\n\n" * 1800
        paragraph = "The optional Vega skill covers shipments. Signing in permits estimates.\n\n"
        fence = "```python\n# Not a real heading\n\nvalue = 7\n```\n\n"
        text = "# Reference\n\n" + padding + "## Vega skill\n\n" + paragraph + fence + padding
        source = {"document_id": "doc", "text": "Signing in permits estimates."}
        result = select_document({"document_id": "doc", "text": text}, [source], radius=80)
        excerpt = result["excerpts"][0]
        self.assertIn(paragraph.strip(), excerpt["text"])
        self.assertIn(fence.strip(), excerpt["text"])
        self.assertEqual(excerpt["text"], text[excerpt["start"] : excerpt["end"]])
        self.assertEqual(excerpt["heading_context"][-1]["text"], "# Reference")
        self.assertIn("## Vega skill", excerpt["text"])

    def test_code_heading_does_not_change_scope(self):
        text = "# Actual\n\n" + ("padding\n\n" * 5000) + "```\n# Fake\n\ntarget\n```\n\n" + ("tail\n\n" * 5000)
        result = select_document(
            {"document_id": "doc", "text": text}, [{"document_id": "doc", "text": "target"}], radius=10
        )
        self.assertEqual(result["excerpts"][0]["heading_context"][-1]["text"], "# Actual")

    def test_ambiguous_or_unmatched_sources_preserve_full_document(self):
        doc = {"document_id": "doc", "text": ("repeated\n\n" * 5000)}
        self.assertIs(select_document(doc, [{"document_id": "doc", "text": "repeated"}]), doc)
        self.assertIs(select_document(doc, [{"document_id": "doc", "text": "absent"}]), doc)

    def test_small_document_unchanged(self):
        doc = {"document_id": "doc", "text": "Small full document."}
        self.assertIs(select_document(doc, []), doc)


if __name__ == "__main__":
    unittest.main()
