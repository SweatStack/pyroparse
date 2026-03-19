import pyarrow as pa

import pyroparse


class TestReadFit:
    def test_returns_table(self, fit_path):
        table = pyroparse.read_fit(fit_path)
        assert isinstance(table, pa.Table)

    def test_row_count(self, fit_path):
        table = pyroparse.read_fit(fit_path)
        assert table.num_rows == 21_666

    def test_schema_types(self, fit_path):
        schema = pyroparse.read_fit(fit_path).schema
        assert schema.field("timestamp").type == pa.timestamp("us", tz="UTC")
        assert schema.field("heart_rate").type == pa.int16()

    def test_accepts_string_path(self, fit_path):
        table = pyroparse.read_fit(str(fit_path))
        assert table.num_rows == 21_666
