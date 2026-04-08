"""
Simple Test for Refactored SEO Rules

Tests the unified validation layer and refactored rules to ensure
false positives are eliminated.
"""

import sys
import os

# Add the parent directories to the path to import modules
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)
sys.path.insert(0, current_dir)

# Mock the seo_rule_utils functions since they might not be available
class MockUtils:
    @staticmethod
    def _normalize_meta_tags(meta_tags):
        if not meta_tags or not isinstance(meta_tags, dict):
            return {}
        normalized = {}
        for key, value in meta_tags.items():
            if key is not None:
                normalized_key = str(key).lower().strip()
                normalized[normalized_key] = value
        return normalized

    @staticmethod
    def _get_meta_tag_value(normalized_meta, tag_name):
        if not normalized_meta:
            return ""
        value = normalized_meta.get(tag_name.lower())
        if value is not None:
            if isinstance(value, list):
                if not value:
                    return ""
                first_item = value[0]
                return str(first_item).strip() if first_item is not None else ""
            return str(value).strip()
        return ""

    @staticmethod
    def _get_schemas(normalized):
        return normalized.get("structured_data", [])

    @staticmethod
    def _find_schema_by_type(schemas, type_name):
        if not schemas or not isinstance(schemas, list):
            return None
        if not type_name:
            return None
        target_type_lower = type_name.lower()
        for s in schemas:
            if isinstance(s, dict):
                schema_type = s.get("@type", "")
                if isinstance(schema_type, list):
                    for t in schema_type:
                        if isinstance(t, str) and t.lower() == target_type_lower:
                            return s
                elif isinstance(schema_type, str):
                    if schema_type.lower() == target_type_lower:
                        return s
        return None

# Patch the imports
import unified_validators
unified_validators._normalize_meta_tags = MockUtils._normalize_meta_tags
unified_validators._get_meta_tag_value = MockUtils._get_meta_tag_value
unified_validators._get_schemas = MockUtils._get_schemas
unified_validators._find_schema_by_type = MockUtils._find_schema_by_type

from unified_validators import (
    check_apple_touch_icon, 
    check_author, 
    check_social_tags, 
    check_security_headers,
    check_pinterest_tags
)


def test_apple_touch_icon():
    """Test Apple Touch Icon validation"""
    print("Testing Apple Touch Icon validation...")
    
    # Test 1: Icon in visual_branding (should be detected)
    data1 = {
        "meta_tags": {},
        "visual_branding": {
            "apple_icons": [
                {"href": "/icon.png", "sizes": "180x180"}
            ]
        }
    }
    result1 = check_apple_touch_icon(data1)
    assert result1['present'] == True, f"Expected present=True, got {result1}"
    assert result1['source'] == 'visual_branding', f"Expected source=visual_branding, got {result1['source']}"
    print("✅ Icon in visual_branding correctly detected")
    
    # Test 2: Icon in meta_tags (should be detected)
    data2 = {
        "meta_tags": {
            "apple-touch-icon": ["/icon.png"]
        },
        "visual_branding": {"apple_icons": []}
    }
    result2 = check_apple_touch_icon(data2)
    assert result2['present'] == True, f"Expected present=True, got {result2}"
    assert result2['source'] == 'meta_tags', f"Expected source=meta_tags, got {result2['source']}"
    print("✅ Icon in meta_tags correctly detected")
    
    # Test 3: No icon anywhere (should be missing)
    data3 = {
        "meta_tags": {},
        "visual_branding": {"apple_icons": []}
    }
    result3 = check_apple_touch_icon(data3)
    assert result3['present'] == False, f"Expected present=False, got {result3}"
    print("✅ Missing icon correctly identified")


def test_author_validation():
    """Test Author validation"""
    print("\nTesting Author validation...")
    
    # Test 1: Author in meta_tags
    data1 = {
        "meta_tags": {"author": ["John Doe"]},
        "structured_data": []
    }
    result1 = check_author(data1)
    assert result1['present'] == True, f"Expected present=True, got {result1}"
    assert result1['source'] == 'meta_tags', f"Expected source=meta_tags, got {result1['source']}"
    print("✅ Author in meta_tags correctly detected")
    
    # Test 2: Author in structured data
    data2 = {
        "meta_tags": {},
        "structured_data": [
            {"@type": "Article", "author": {"name": "Jane Smith"}}
        ]
    }
    result2 = check_author(data2)
    assert result2['present'] == True, f"Expected present=True, got {result2}"
    assert result2['source'] == 'structured_data', f"Expected source=structured_data, got {result2['source']}"
    print("✅ Author in structured data correctly detected")
    
    # Test 3: Person schema with name
    data3 = {
        "meta_tags": {},
        "structured_data": [
            {"@type": "Person", "name": "Bob Johnson"}
        ]
    }
    result3 = check_author(data3)
    assert result3['present'] == True, f"Expected present=True, got {result3}"
    print("✅ Person schema author correctly detected")
    
    # Test 4: No author anywhere
    data4 = {
        "meta_tags": {},
        "structured_data": []
    }
    result4 = check_author(data4)
    assert result4['present'] == False, f"Expected present=False, got {result4}"
    print("✅ Missing author correctly identified")


def test_social_tags():
    """Test Social Tags validation"""
    print("\nTesting Social Tags validation...")
    
    # Test 1: OG tags in meta_tags
    data1 = {
        "meta_tags": {
            "og:title": ["Test Page"],
            "og:description": ["Test Description"],
            "og:image": ["https://example.com/image.jpg"],
            "og:url": ["https://example.com/page"]
        },
        "social": {}
    }
    result1 = check_social_tags(data1)
    assert result1['present'] == True, f"Expected present=True, got {result1}"
    assert len(result1['og_tags']) == 4, f"Expected 4 OG tags, got {len(result1['og_tags'])}"
    print("✅ OG tags in meta_tags correctly detected")
    
    # Test 2: Missing OG tags
    data2 = {
        "meta_tags": {},
        "social": {"open_graph": {}}
    }
    result2 = check_social_tags(data2)
    assert result2['present'] == False, f"Expected present=False, got {result2}"
    assert len(result2['missing_required']) > 0, "Expected missing required tags"
    print("✅ Missing OG tags correctly identified")


def test_pinterest_tags():
    """Test Pinterest tags validation"""
    print("\nTesting Pinterest tags validation...")
    
    # Test 1: Pinterest tags present
    data1 = {
        "meta_tags": {
            "pin:media": ["https://example.com/image.jpg"],
            "pin:description": ["Test Pin Description"]
        }
    }
    result1 = check_pinterest_tags(data1)
    assert result1['present'] == True, f"Expected present=True, got {result1}"
    assert result1['severity'] == 'info', f"Expected severity=info, got {result1['severity']}"
    print("✅ Pinterest tags correctly detected as optional")
    
    # Test 2: No Pinterest tags
    data2 = {"meta_tags": {}}
    result2 = check_pinterest_tags(data2)
    assert result2['present'] == False, f"Expected present=False, got {result2}"
    assert result2['severity'] == 'info', f"Expected severity=info, got {result2['severity']}"
    print("✅ Missing Pinterest tags correctly identified as optional")


def test_security_headers():
    """Test Security Headers validation"""
    print("\nTesting Security Headers validation...")
    
    # Test 1: Some security headers present
    data1 = {
        "headers": {
            "x-frame-options": "DENY",
            "x-content-type-options": "nosniff"
        },
        "meta_tags": {}
    }
    result1 = check_security_headers(data1)
    assert len(result1['present']) >= 2, f"Expected at least 2 headers present, got {len(result1['present'])}"
    assert result1['severity'] == 'info', f"Expected severity=info, got {result1['severity']}"
    print("✅ Security headers correctly detected")
    
    # Test 2: No security headers
    data2 = {"headers": {}, "meta_tags": {}}
    result2 = check_security_headers(data2)
    assert len(result2['missing']) > 0, f"Expected missing headers, got {result2['missing']}"
    assert result2['severity'] == 'info', f"Expected severity=info, got {result2['severity']}"
    print("✅ Missing security headers correctly identified as optional")


def run_all_tests():
    """Run all tests"""
    print("🧪 Testing Refactored SEO Rule Engine")
    print("=" * 50)
    
    try:
        test_apple_touch_icon()
        test_author_validation()
        test_social_tags()
        test_pinterest_tags()
        test_security_headers()
        
        print("\n" + "=" * 50)
        print("🎉 ALL TESTS PASSED!")
        print("✅ False positives eliminated")
        print("✅ Unified validation layer working")
        print("✅ Data structure mismatches fixed")
        
    except AssertionError as e:
        print(f"\n❌ TEST FAILED: {e}")
        return False
    except Exception as e:
        print(f"\n💥 UNEXPECTED ERROR: {e}")
        return False
    
    return True


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
