import importlib.util
import sys
import unittest
from pathlib import Path

sys.dont_write_bytecode = True

SPEC = importlib.util.spec_from_file_location("check_java_comments", Path(__file__).with_name("check-java-comments.py"))
checker = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(checker)


class JavaCommentPolicyTest(unittest.TestCase):
    def test_new_implementation_comment_is_rejected(self):
        self.assertEqual(["// new explanation"], checker.violations("// new explanation\nclass Example {}", []))

    def test_baseline_comment_remains_allowed(self):
        self.assertEqual([], checker.violations("// existing explanation\nclass Example {}", ["// existing explanation"]))

    def test_public_api_javadoc_is_allowed(self):
        self.assertEqual([], checker.violations("/** Public API documentation. */\npublic class Example {}", []))

    def test_suppression_with_adjacent_reason_is_allowed(self):
        self.assertEqual([], checker.violations('void run() { @SuppressWarnings("rule") // required by API\n }', []))

    def test_repository_matches_empty_baseline(self):
        self.assertEqual({}, checker.json.loads(checker.BASELINE.read_text(encoding="utf-8")))
        self.assertEqual(0, checker.main())


if __name__ == "__main__":
    unittest.main()
