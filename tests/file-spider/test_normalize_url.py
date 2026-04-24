# -*- coding: utf-8 -*-
"""
normalize_url 纯函数单元测试

仅依赖 feapder.utils.tools.normalize_url（背后用到 urllib + 真实 w3lib），
不引入 FileSpider，无需任何 monkeypatch 桩。
"""

import unittest

from feapder.utils.tools import normalize_url


class TestNormalizeUrl(unittest.TestCase):
    def test_aliyun_oss(self):
        url = "https://oss.example.com/img/a.jpg?Expires=1761000000&Signature=xxx&OSSAccessKeyId=yyy"
        self.assertEqual(normalize_url(url), "https://oss.example.com/img/a.jpg")

    def test_aws_prefix_match_is_case_insensitive(self):
        url = (
            "https://bucket.s3.amazonaws.com/key.png"
            "?x-amz-security-token=sts"
            "&X-Amz-Signature=abc"
            "&biz=1"
        )
        self.assertEqual(normalize_url(url), "https://bucket.s3.amazonaws.com/key.png?biz=1")

    def test_aws_v2_query_sign_params_are_stripped(self):
        url = (
            "https://bucket.s3.amazonaws.com/key.png"
            "?AWSAccessKeyId=AKIAEXAMPLE"
            "&Signature=abc"
            "&Expires=1761000000"
            "&biz=1"
        )
        self.assertEqual(normalize_url(url), "https://bucket.s3.amazonaws.com/key.png?biz=1")

    def test_tencent_cos(self):
        url = (
            "https://bucket.cos.ap-shanghai.myqcloud.com/file.pdf"
            "?q-sign-algorithm=sha1&q-ak=AKID&q-sign-time=1761000000;1761003600"
            "&q-key-time=1761000000;1761003600&q-header-list=host"
            "&q-url-param-list=&q-signature=deadbeef"
        )
        self.assertEqual(normalize_url(url), "https://bucket.cos.ap-shanghai.myqcloud.com/file.pdf")

    def test_keep_business_params_and_sort_query(self):
        url = (
            "https://bucket.s3.amazonaws.com/a.png"
            "?page=2&biz=1&X-Amz-Date=20260423T000000Z&X-Amz-Signature=abc#frag"
        )
        self.assertEqual(
            normalize_url(url),
            "https://bucket.s3.amazonaws.com/a.png?biz=1&page=2",
        )

    def test_default_is_conservative_for_generic_fields(self):
        url = "https://example.com/x?token=abc&sign=2&timestamp=3&biz=1"
        self.assertEqual(
            normalize_url(url),
            "https://example.com/x?biz=1&sign=2&timestamp=3&token=abc",
        )

    def test_custom_strip_params_supports_prefix_pattern(self):
        url = "https://example.com/x?token=abc&q-signature=1&q-ak=2&biz=1"
        self.assertEqual(
            normalize_url(url, strip_params={"token", "q-*"}),
            "https://example.com/x?biz=1",
        )

    def test_strip_all_with_wildcard(self):
        """strip_params={"*"} 应剥光所有 query 参数（前缀通配的自然延伸）"""
        url = "https://example.com/x?a=1&b=2&c=3"
        self.assertEqual(normalize_url(url, strip_params={"*"}), "https://example.com/x")

    def test_only_path(self):
        url = "https://oss.example.com/img/a.jpg?biz=1&t=123#frag"
        self.assertEqual(
            normalize_url(url, only_path=True),
            "https://oss.example.com/img/a.jpg",
        )

    def test_no_query_still_uses_canonicalize_semantics(self):
        url = "https://example.com/x?"
        self.assertEqual(normalize_url(url), "https://example.com/x")

    def test_empty(self):
        self.assertEqual(normalize_url(""), "")
        self.assertIsNone(normalize_url(None))


if __name__ == "__main__":
    unittest.main()
