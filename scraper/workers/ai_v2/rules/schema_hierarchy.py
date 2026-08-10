"""
Schema.org type hierarchy for V2 coverage validation.

Single source of truth for schema.org inheritance relationships used by
GEO schema coverage rules. Enables hierarchy-aware type matching so that,
for example, a page with @type "LocalBusiness" satisfies an "Organization"
requirement, and a "Restaurant" satisfies both "LocalBusiness" and
"Organization".

Public API
----------
satisfies_type(actual_types, expected_type) -> bool
    True if any type in actual_types is equal to or a subtype of
    expected_type per the schema.org hierarchy.

matching_type(actual_types, expected_type) -> str | None
    Returns the first actual type that satisfies expected_type, or None.
    Useful for populating rule evidence (e.g. "matched_type": "LocalBusiness").

Design notes
------------
- Only types relevant to GEO schema coverage rules are listed.
- Unknown types (not in _PARENT) are treated as having no ancestors; they
  only satisfy themselves, so satisfies_type(["Foo"], "Organization") is
  False for any unknown "Foo".
- Comparisons are case-insensitive to tolerate minor @type casing variations
  in publisher JSON-LD.
- This module has no side effects and no imports from the rule engine.
  Import it freely from rules, extractors, or API controllers.

Counterpart
-----------
The flat ORG_SUBTYPES list in aiHubController.js mirrors the Organization
branch of this hierarchy for use in MongoDB aggregation pipelines.
Update both files when adding new types.
"""

from __future__ import annotations
from typing import Sequence


# Maps each schema.org type to its direct parent in the hierarchy.
# None = root type within this sub-graph (no parent to walk to).
_PARENT: dict[str, str | None] = {

    # ── Organization branch ───────────────────────────────────────────────────
    "Organization":            None,
    "Corporation":             "Organization",
    "NGO":                     "Organization",
    "GovernmentOrganization":  "Organization",
    "EducationalOrganization": "Organization",
    "MedicalOrganization":     "Organization",
    "NewsMediaOrganization":   "Organization",
    "OnlineBusiness":          "Organization",
    "PerformingGroup":         "Organization",
    "SportsOrganization":      "Organization",

    # LocalBusiness (direct child of Organization)
    "LocalBusiness":                "Organization",
    "AutoDealer":                   "LocalBusiness",
    "HealthAndBeautyBusiness":      "LocalBusiness",
    "HomeAndConstructionBusiness":  "LocalBusiness",
    "LodgingBusiness":              "LocalBusiness",
    "EntertainmentBusiness":        "LocalBusiness",
    "FoodEstablishment":            "LocalBusiness",
    "Store":                        "LocalBusiness",
    "FinancialService":             "LocalBusiness",
    "SportsActivityLocation":       "LocalBusiness",
    "MedicalClinic":                "LocalBusiness",
    "Dentist":                      "LocalBusiness",
    "LegalService":                 "LocalBusiness",
    "AnimalShelter":                "LocalBusiness",
    "ChildCare":                    "LocalBusiness",
    "DryCleaningOrLaundry":         "LocalBusiness",
    "EmergencyService":             "LocalBusiness",
    "EmploymentAgency":             "LocalBusiness",

    # HealthAndBeautyBusiness subtypes
    "HairSalon":   "HealthAndBeautyBusiness",
    "BeautySalon": "HealthAndBeautyBusiness",
    "NailSalon":   "HealthAndBeautyBusiness",
    "TattooParlor":"HealthAndBeautyBusiness",

    # FoodEstablishment subtypes
    "Restaurant":         "FoodEstablishment",
    "CafeOrCoffeeShop":   "FoodEstablishment",
    "FastFoodRestaurant": "FoodEstablishment",
    "Bakery":             "FoodEstablishment",
    "BarOrPub":           "FoodEstablishment",
    "IceCreamShop":       "FoodEstablishment",

    # FastFoodRestaurant subtypes
    # (schema.org lists these under Restaurant, but FastFoodRestaurant is a
    #  well-known type; both paths reach Organization)
    "FastFoodRestaurant": "Restaurant",

    # LodgingBusiness subtypes
    "BedAndBreakfast": "LodgingBusiness",
    "Campground":      "LodgingBusiness",
    "Hostel":          "LodgingBusiness",
    "Hotel":           "LodgingBusiness",
    "Motel":           "LodgingBusiness",
    "Resort":          "LodgingBusiness",
    "VacationRental":  "LodgingBusiness",

    # ── CreativeWork / Article branch ─────────────────────────────────────────
    "CreativeWork": None,
    "Article":      "CreativeWork",
    "BlogPosting":  "Article",
    "NewsArticle":  "Article",
    "TechArticle":  "Article",
    "HowTo":        "CreativeWork",

    # ── WebPage branch ────────────────────────────────────────────────────────
    "WebPage":     None,
    "FAQPage":     "WebPage",
    "QAPage":      "WebPage",
    "AboutPage":   "WebPage",
    "ContactPage": "WebPage",
    "ItemPage":    "WebPage",

    # ── Standalone root types ─────────────────────────────────────────────────
    "Service": None,
    "Product": None,
    "Person":  None,
}


def _ancestors(type_name: str) -> frozenset[str]:
    """
    Return the type itself plus every ancestor, following _PARENT pointers.

    For types not in _PARENT, returns {type_name} — the type only satisfies
    itself (no inheritance chain is known for it).

    The loop guard prevents infinite cycles if _PARENT is ever misconfigured.
    """
    result: set[str] = set()
    current: str | None = type_name
    seen:    set[str] = set()
    while current is not None and current not in seen:
        result.add(current)
        seen.add(current)
        current = _PARENT.get(current)
    # If the type is unknown, _PARENT.get returns KeyError-default None,
    # so the loop exits after adding just type_name itself.
    if not result:
        result.add(type_name)
    return frozenset(result)


def satisfies_type(actual_types: Sequence[str], expected_type: str) -> bool:
    """
    Return True if any type in actual_types is equal to or a subtype of
    expected_type per the schema.org hierarchy.

    Comparison is case-insensitive.

    Examples::

        satisfies_type(["LocalBusiness"], "Organization")  → True
        satisfies_type(["Restaurant"],    "Organization")  → True
        satisfies_type(["MedicalClinic"], "Organization")  → True
        satisfies_type(["Organization"],  "Organization")  → True
        satisfies_type(["LocalBusiness"], "Service")       → False
        satisfies_type(["Article"],       "Organization")  → False
        satisfies_type(["LocalBusiness"], "LocalBusiness") → True
        satisfies_type([],                "Organization")  → False
    """
    expected_lower = expected_type.lower()
    for actual in actual_types:
        if not actual:
            continue
        if any(a.lower() == expected_lower for a in _ancestors(actual)):
            return True
    return False


def matching_type(actual_types: Sequence[str], expected_type: str) -> str | None:
    """
    Return the first type in actual_types that satisfies expected_type,
    or None if none does.

    Useful for populating evidence fields in rule results to show which
    concrete type satisfied a broader requirement.

    Example::

        matching_type(["LocalBusiness"], "Organization")  → "LocalBusiness"
        matching_type(["Restaurant"],    "Organization")  → "Restaurant"
        matching_type(["Article"],       "Organization")  → None
    """
    expected_lower = expected_type.lower()
    for actual in actual_types:
        if not actual:
            continue
        if any(a.lower() == expected_lower for a in _ancestors(actual)):
            return actual
    return None
