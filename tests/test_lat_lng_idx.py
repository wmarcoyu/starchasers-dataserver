"""Test latitude longitude translations to grid/matrix indices.

For 0p25 resolution, matrix shape is (721, 1440).
"""
from dataserver.model import get_lat_idx, get_lng_idx


def test_lat_idx():
    """Test latitude to latitude index translation.

    Step size is 0.25: 90, 89.75, 89.5, ..., -89.75, -90.
    """
    assert get_lat_idx(90) == 0
    assert get_lat_idx(90.000) == 0
    # equal distance - should be index 0
    assert get_lat_idx(89.875) == 0
    assert get_lat_idx(89.876) == 0
    assert get_lat_idx(89.874) == 1
    assert get_lat_idx(89.75) == 1

    assert get_lat_idx(-90) == 720
    assert get_lat_idx(-90.000) == 720
    # equal distance - should be index 719
    assert get_lat_idx(-89.875) == 719
    assert get_lat_idx(-89.874) == 719
    assert get_lat_idx(-89.876) == 720
    assert get_lat_idx(-89.75) == 719

    assert get_lat_idx(0) == 360
    # equal distance - should be index 359
    assert get_lat_idx(0.125) == 359
    assert get_lat_idx(-0.125) == 360

    try:
        get_lat_idx(90.01)
    except ValueError as error:
        print(f"Caught ValueError {error}")

    try:
        get_lat_idx(-100)
    except ValueError as error:
        print(f"Caught ValueError {error}")


def test_lng_idx():
    """Test longitude to longitude index translation.

    Step size is 0.25: 0, 0.25, 0.5, ..., 359.5, 359.75.
    Thus get_lng_idx needs to first offset lng by 180 degrees.
    """
    assert get_lng_idx(-180) == 0
    # equal distance - index 0
    assert get_lng_idx(-179.875) == 0
    assert get_lng_idx(-179.9) == 0
    assert get_lng_idx(-179.874) == 1
    assert get_lng_idx(-179.75) == 1

    assert get_lng_idx(180) == 1439
    # last precise longitude - 359.75
    assert get_lng_idx(179.75) == 1439
    # equal distance - index 1438
    assert get_lng_idx(179.625) == 1438

    # longitude 0 - index 720
    assert get_lng_idx(0) == 720
    # equal distance - index 719
    assert get_lng_idx(-0.125) == 719
    assert get_lng_idx(-0.124) == 720
    assert get_lng_idx(0.124) == 720
    # equal distance - index 720
    assert get_lng_idx(0.125) == 720

    try:
        get_lng_idx(-200)
    except ValueError as error:
        print(f"Caught ValueError {error}")

    try:
        get_lng_idx(360)
    except ValueError as error:
        print(f"Caught ValueError {error}")
