"""Immutable domain configuration for the MandiBhav ETL."""

from typing import Dict, List

PRIORITY_COMMODITIES: Dict[int, str] = {
    1: "Wheat", 2: "Paddy(Dhan)(Common)", 3: "Maize", 4: "Bengal Gram(Gram)(Whole)",
    5: "Jowar(Sorghum)", 6: "Bajra(Pearl Millet/Cumbu)", 8: "Barley(Jau)", 9: "Ragi(Finger Millet)",
    10: "Green Gram(Moong)(Whole)", 11: "Black Gram(Urd Beans)(Whole)", 12: "Mustard", 13: "Soyabean",
    14: "Groundnut", 15: "Cotton", 18: "Arhar (Tur/Red Gram)", 19: "Masur (Lentil)", 22: "Garlic",
    23: "Onion", 24: "Potato", 25: "Ginger(Green)", 26: "Chilli(Green)", 27: "Chilli(Dry)",
    28: "Tomato", 32: "Apple", 33: "Mango", 45: "Banana", 51: "Coriander(Leaves)",
    61: "Cumin Seed(Jeera)", 65: "Turmeric",
}

TOP_STAPLE_STATES: List[int] = [19, 34, 28, 12, 29, 11, 20, 16, 1, 36, 31, 35, 26, 4, 6]

STATE_NAME_BY_ID: Dict[int, str] = {
    1: "Andhra Pradesh", 3: "Assam", 4: "Bihar", 6: "Chhattisgarh", 11: "Gujarat",
    12: "Haryana", 13: "Himachal Pradesh", 14: "Jharkhand", 15: "Jammu and Kashmir",
    16: "Karnataka", 17: "Kerala", 19: "Madhya Pradesh", 20: "Maharashtra", 26: "Odisha",
    28: "Punjab", 29: "Rajasthan", 31: "Tamil Nadu", 33: "Uttarakhand", 34: "Uttar Pradesh",
    35: "West Bengal", 36: "Telangana",
}

PRODUCING_STATES: Dict[int, List[int]] = {
    1: [19, 34, 28, 29, 11, 20, 12, 4, 6], 2: [28, 34, 19, 12, 11, 20, 16, 1, 36, 31, 35, 26, 4, 6, 3],
    3: [19, 20, 16, 29, 11, 34, 1, 36, 4, 6], 4: [19, 20, 29, 11, 16, 34, 1, 36, 12],
    5: [20, 16, 19, 29, 1, 36, 31], 6: [29, 34, 12, 11, 19, 20, 31], 8: [29, 34, 19, 28, 12],
    9: [16, 31, 1, 36, 20, 26], 10: [29, 19, 20, 16, 11, 34, 12, 1, 36],
    11: [19, 34, 20, 16, 11, 1, 36, 31, 35], 12: [29, 19, 12, 34, 11, 35, 4, 6],
    13: [19, 20, 29, 16, 11, 36, 6], 14: [11, 1, 31, 16, 29, 20, 36, 26],
    15: [20, 11, 36, 1, 29, 28, 12, 16, 19], 18: [20, 16, 19, 11, 34, 1, 36, 26, 4],
    19: [19, 34, 35, 4, 29], 22: [19, 29, 11, 34, 20, 12], 23: [20, 19, 11, 29, 16, 34, 1, 36, 31, 12],
    24: [34, 35, 4, 28, 19, 11, 20, 29, 12, 16], 25: [19, 16, 20, 11, 35, 3, 26],
    26: [1, 36, 16, 20, 19, 11, 34, 35, 31], 27: [1, 36, 16, 20, 19, 11, 31],
    28: [20, 16, 19, 11, 34, 29, 1, 36, 31, 26, 6], 32: [15, 13, 33],
    33: [34, 1, 16, 11, 20, 35, 31, 19], 45: [31, 20, 11, 1, 16, 19, 36, 35, 4],
    51: [19, 29, 11, 34, 20, 16], 61: [11, 29], 65: [36, 31, 20, 1, 26, 16, 35, 3],
}
