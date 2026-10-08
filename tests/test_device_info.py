"""DeviceInfo decoding: what a device is called, and which device is which.

- ``product`` is a key: the name the profile's ``product`` subfield gives it for
  the device's manufacturer (``garmin_product`` for garmin, dynastream,
  dynastream_oem and tacx; ``favero_product`` for favero_electronics), else the
  raw number. ``product_name`` is the file's free text, kept verbatim.
- ``name`` joins the manufacturer and the product, without display formatting.
- A message under a ``device_index`` already in use is a re-emission only when
  no identity field disagrees; a head unit may hand the index to a new sensor.
- A column goes to the matching channel emitted last, and a dedicated sensor
  (a heart-rate strap) is never credited by manufacturer with another column.
  A channel's type is read per its ``source_type``: ANT+ and Bluetooth number
  the same sensors differently.
"""

from datetime import datetime, timedelta, timezone

import pytest

from pyroparse import Activity
from pyroparse._metadata import _device_name

from fit_builder import (
    RECORD,
    SINT32,
    UINT8,
    UINT16,
    UINT32,
    FitBuilder,
    activity,
    device_info,
    file_id,
    fit_time,
    session,
)

START = datetime(2024, 6, 1, 10, 0, tzinfo=timezone.utc)

# FIT profile values.
GARMIN, WAHOO, TACX, STRYD, FAVERO = 1, 32, 89, 95, 263
FENIX7, TACX_NEO_SMART, ASSIOMA_DUO = 3906, 4265, 12
HEART_RATE, BIKE_POWER = 120, 11  # ANT+ device types
BLE, BLE_HEART_RATE = 3, 1  # source_type bluetooth_low_energy, ble_device_type heart_rate


def _build(*devices: dict, power: bool = False) -> bytes:
    """An activity with three Records (heart rate, optionally power) and the
    given device_info emissions, written in order before the records."""
    b = FitBuilder()
    file_id(b, 0)
    for d in devices:
        device_info(b, 1, **d)
    fields = [(253, UINT32, 4), (3, UINT8, 1)] + ([(7, UINT16, 2)] if power else [])
    b.define(2, RECORD, fields)
    for i in range(3):
        b.data(2, [fit_time(START) + i, 120] + ([200] if power else []))
    session(b, 3, START, 3, sport=2)
    activity(b, 4, START + timedelta(seconds=3), 0)
    return b.build()


def _devices(*devices: dict, power: bool = False):
    return Activity.load_fit(_build(*devices, power=power)).metadata.devices


def _sensor(**fields):
    (device,) = [d for d in _devices(dict(index=0, manufacturer=GARMIN), fields) if d.device_type == "sensor"]
    return device


class TestProduct:
    def test_garmin_product_enum(self):
        creator = _devices(dict(index=0, manufacturer=GARMIN, product=FENIX7, serial=1))[0]
        assert (creator.product, creator.name) == ("fenix7", "garmin fenix7")

    def test_favero_product_subfield(self):
        d = _sensor(index=1, manufacturer=FAVERO, product=ASSIOMA_DUO, serial=7)
        assert d.product == "assioma_duo"

    def test_tacx_uses_the_garmin_product_subfield(self):
        d = _sensor(index=1, manufacturer=TACX, product=TACX_NEO_SMART, serial=7)
        assert d.product == "tacx_neo_smart"
        assert d.name == "tacx_neo_smart"  # the product already names the maker

    def test_product_without_a_subfield_is_its_number(self):
        d = _sensor(index=1, manufacturer=STRYD, product=1, serial=7)
        assert (d.product, d.name) == ("1", "stryd")

    def test_unknown_garmin_product_is_its_number(self):
        d = _sensor(index=1, manufacturer=GARMIN, product=65000, serial=7)
        assert (d.product, d.name) == ("65000", "garmin")

    def test_product_name_is_kept_verbatim_beside_the_enum(self):
        d = _sensor(index=1, manufacturer=FAVERO, product=ASSIOMA_DUO, serial=7, product_name="Power")
        assert (d.product, d.product_name) == ("assioma_duo", "Power")
        assert d.name == "favero_electronics assioma_duo"

    def test_product_name_names_a_device_without_a_product_enum(self):
        d = _sensor(index=1, manufacturer=GARMIN, serial=7, product_name="HRM-Pro:620385")
        assert d.product is None
        assert d.product_name == "HRM-Pro:620385"
        assert d.name == "garmin HRM-Pro:620385"

    def test_channel_with_only_a_label(self):
        d = _sensor(index=1, product_name="Temperature")
        assert (d.manufacturer, d.name) == (None, "Temperature")


class TestDeviceName:
    @pytest.mark.parametrize(
        "manufacturer,product,product_name,expected",
        [
            ("garmin", "hrm_pro", None, "garmin hrm_pro"),
            ("garmin", "hrm_pro", "HRM-Pro:620385", "garmin hrm_pro"),
            ("garmin", "4614", "NERA", "garmin NERA"),
            ("garmin", "4614", None, "garmin"),
            ("wahoo_fitness", None, "ELEMNT ROAM", "wahoo_fitness ELEMNT ROAM"),
            ("concept2", None, "Concept2", "Concept2"),
            ("core", None, "CORE", "CORE"),
            ("tacx", "tacx_training_app_mac_catalyst", None, "tacx_training_app_mac_catalyst"),
            ("core", None, "Coremark", "core Coremark"),  # a prefix, not a word
            (None, None, "Temperature", "Temperature"),
            (None, None, None, None),
        ],
    )
    def test_rule(self, manufacturer, product, product_name, expected):
        assert _device_name(manufacturer, product, product_name) == expected


class TestReusedIndex:
    def test_another_serial_under_a_reused_index_is_another_device(self):
        devices = _devices(
            dict(index=0, manufacturer=GARMIN),
            dict(index=2, manufacturer=FAVERO, product=ASSIOMA_DUO, serial=111),
            dict(index=2, manufacturer=FAVERO, product=ASSIOMA_DUO, serial=222),
            dict(index=2, manufacturer=FAVERO, product=ASSIOMA_DUO, serial=111),
        )
        assert [d.serial_number for d in devices if d.device_type == "sensor"] == ["111", "222"]

    def test_a_sparse_re_emission_fills_gaps(self):
        devices = _devices(
            dict(index=0, manufacturer=GARMIN),
            dict(index=2, manufacturer=FAVERO, serial=111),
            dict(index=2, product=ASSIOMA_DUO, device_type=BIKE_POWER),
        )
        (pedals,) = [d for d in devices if d.device_type == "sensor"]
        assert (pedals.serial_number, pedals.product) == ("111", "assioma_duo")

    def test_index_handed_to_another_sensor(self):
        """Wahoo: index 2 is a power meter, then a heart-rate strap; index 3 the
        other way round, then a second power meter. Five devices, not three;
        each column goes to the matching channel emitted last."""
        devices = _devices(
            dict(index=0, manufacturer=WAHOO, product_name="ELEMNT ROAM", serial=1),
            dict(index=2, manufacturer=FAVERO, product=ASSIOMA_DUO, serial=111, device_type=BIKE_POWER),
            dict(index=3, manufacturer=WAHOO, product_name="TICKR", device_type=HEART_RATE),
            dict(index=2, manufacturer=WAHOO, product_name="TICKR", device_type=HEART_RATE),
            dict(index=3, manufacturer=FAVERO, product=ASSIOMA_DUO, serial=111, device_type=BIKE_POWER),
            dict(index=3, manufacturer=FAVERO, product=ASSIOMA_DUO, serial=222, device_type=BIKE_POWER),
            power=True,
        )
        by_label = {(d.name, d.serial_number): d.columns for d in devices}
        assert by_label == {
            ("wahoo_fitness ELEMNT ROAM", "1"): [],
            ("favero_electronics assioma_duo", "111"): [],
            ("wahoo_fitness TICKR", None): ["heart_rate"],
            ("favero_electronics assioma_duo", "222"): ["power"],
        }


class TestAttribution:
    def test_a_heart_rate_strap_is_not_a_power_source_by_manufacturer(self):
        """Wahoo makes KICKR trainers, so `wahoo_fitness` is a known power
        manufacturer, but a TICKR declares itself a heart-rate sensor."""
        devices = _devices(
            dict(index=0, manufacturer=GARMIN, product=FENIX7, serial=1),
            dict(index=2, manufacturer=FAVERO, product=ASSIOMA_DUO, serial=111, device_type=BIKE_POWER),
            dict(index=3, manufacturer=WAHOO, product_name="TICKR", device_type=HEART_RATE),
            power=True,
        )
        assert {d.name: d.columns for d in devices} == {
            "garmin fenix7": [],
            "favero_electronics assioma_duo": ["power"],
            "wahoo_fitness TICKR": ["heart_rate"],
        }


    def test_a_bluetooth_strap_is_matched_by_its_bluetooth_type(self):
        devices = _devices(
            dict(index=0, manufacturer=GARMIN, product=FENIX7, serial=1),
            dict(index=2, manufacturer=FAVERO, product=ASSIOMA_DUO, serial=111, device_type=BIKE_POWER, source_type=1),
            dict(index=3, manufacturer=WAHOO, product_name="TICKR", device_type=BLE_HEART_RATE, source_type=BLE),
            power=True,
        )
        assert {d.name: d.columns for d in devices} == {
            "garmin fenix7": [],
            "favero_electronics assioma_duo": ["power"],
            "wahoo_fitness TICKR": ["heart_rate"],
        }


    def test_an_ant_type_on_a_bluetooth_channel(self):
        """Wahoo head units write the ANT+ type (120) on a Bluetooth channel."""
        devices = _devices(
            dict(index=0, manufacturer=WAHOO, product_name="ELEMNT BOLT", serial=1),
            dict(index=1, product_name="Heart Rate", serial=7, device_type=HEART_RATE, source_type=BLE),
        )
        assert {d.name: d.columns for d in devices} == {
            "wahoo_fitness ELEMNT BOLT": [],
            "Heart Rate": ["heart_rate"],
        }


class TestEveryPathAgrees:
    DEVICES = (
        dict(index=0, manufacturer=GARMIN, product=FENIX7, serial=1),
        dict(index=2, manufacturer=FAVERO, product=ASSIOMA_DUO, serial=111, product_name="Power"),
        dict(index=2, manufacturer=WAHOO, product_name="TICKR", device_type=HEART_RATE),
    )

    @staticmethod
    def _identities(devices):
        return [(d.name, d.manufacturer, d.product, d.product_name, d.serial_number) for d in devices]

    def test_open_fit_lists_what_load_fit_lists(self, tmp_path):
        path = tmp_path / "devices.fit"
        path.write_bytes(_build(*self.DEVICES))
        assert self._identities(Activity.open_fit(path).metadata.devices) == self._identities(
            Activity.load_fit(path).metadata.devices
        )

    def test_parquet_round_trip(self, tmp_path):
        loaded = Activity.load_fit(_build(*self.DEVICES))
        loaded.to_parquet(tmp_path / "a.parquet")
        restored = Activity.load_parquet(tmp_path / "a.parquet")
        assert restored.metadata.devices == loaded.metadata.devices


class TestFootPod:
    """A foot pod (here an HRM-Pro's running channel, registered in every sport)
    is a source only on foot: for cadence always, for speed and distance only
    without GPS. Outdoors the watch uses GPS unless the runner chose the foot
    pod, which the file does not record; pyroparse then credits the watch."""

    HRM_PRO_RUNNING = dict(index=2, manufacturer=GARMIN, serial=7, device_type=124, source_type=1)

    def _attributed(self, sport: int, gps: bool) -> dict:
        b = FitBuilder()
        file_id(b, 0)
        device_info(b, 1, 0, manufacturer=GARMIN, product=FENIX7, serial=1)
        device_info(b, 1, **self.HRM_PRO_RUNNING)
        fields = [(253, UINT32, 4), (4, UINT8, 1), (5, UINT32, 4), (6, UINT16, 2)]
        if gps:
            fields += [(0, SINT32, 4), (1, SINT32, 4)]
        b.define(2, RECORD, fields)
        for i in range(3):
            values = [fit_time(START) + i, 85, 300 * i, 3000]  # cadence, distance (cm), speed (mm/s)
            b.data(2, values + ([600_000_000 + i, 50_000_000] if gps else []))
        session(b, 3, START, 3, sport=sport)
        activity(b, 4, START + timedelta(seconds=3), 0)
        devices = Activity.load_fit(b.build()).metadata.devices
        return {d.device_type: d.columns for d in devices}

    def test_outdoor_run(self):
        assert self._attributed(sport=1, gps=True)["sensor"] == ["cadence"]

    def test_treadmill_run(self):
        assert self._attributed(sport=1, gps=False)["sensor"] == ["cadence", "speed", "distance"]

    def test_ride(self):
        attributed = self._attributed(sport=2, gps=False)
        assert attributed["sensor"] == []
        assert {"cadence", "speed", "distance"} <= set(attributed["creator"])
