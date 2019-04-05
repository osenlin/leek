import unittest

from numpy.testing import assert_array_equal
from numpy.testing import assert_array_almost_equal


__all__ = ["assert_equal", "assert_not_equal", "assert_true",
           "assert_false",  "assert_raises", "SkipTest",
           "assert_dict_equal", "assert_in", "assert_not_in",
           "assert_less", "assert_greater", "assert_less_equal",
           "assert_greater_equal", "assert_array_equal",
           "assert_array_almost_equal"]


_dummy = unittest.TestCase('__init__')
assert_equal = _dummy.assertEqual
assert_not_equal = _dummy.assertNotEqual
assert_true = _dummy.assertTrue
assert_false = _dummy.assertFalse
assert_raises = _dummy.assertRaises
SkipTest = unittest.case.SkipTest
assert_dict_equal = _dummy.assertDictEqual
assert_in = _dummy.assertIn
assert_not_in = _dummy.assertNotIn
assert_less = _dummy.assertLess
assert_greater = _dummy.assertGreater
assert_less_equal = _dummy.assertLessEqual
assert_greater_equal = _dummy.assertGreaterEqual
