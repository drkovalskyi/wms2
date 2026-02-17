"""Unit tests for LFN derivation helpers."""

from wms2.core.output_lfn import (
    derive_merged_lfn_bases,
    local_output_path,
    merged_lfn_for_group,
)


class TestDeriveMergedLfnBases:
    def test_real_request_data(self):
        """Real-style request data → correct merged LFN bases."""
        request_data = {
            "MergedLFNBase": "/store/mc",
            "OutputDatasets": [
                "/QCD_PT-15to7000/RunIII2024-PremixRaw_140X-v2/AODSIM",
                "/QCD_PT-15to7000/RunIII2024-NanoAOD_140X-v2/NANOAODSIM",
            ],
        }
        result = derive_merged_lfn_bases(request_data)
        assert len(result) == 2

        # First dataset
        assert result[0]["dataset_name"] == "/QCD_PT-15to7000/RunIII2024-PremixRaw_140X-v2/AODSIM"
        assert result[0]["merged_lfn_base"] == (
            "/store/mc/RunIII2024/QCD_PT-15to7000/AODSIM/PremixRaw_140X-v2"
        )
        assert result[0]["primary_dataset"] == "QCD_PT-15to7000"
        assert result[0]["data_tier"] == "AODSIM"

        # Second dataset
        assert result[1]["data_tier"] == "NANOAODSIM"

    def test_no_output_modules_returns_empty(self):
        """No OutputDatasets → empty list."""
        result = derive_merged_lfn_bases({"MergedLFNBase": "/store/mc"})
        assert result == []

    def test_filters_kept_outputs_only(self):
        """Only OutputDatasets entries are returned (they represent KeepOutput=true steps)."""
        request_data = {
            "MergedLFNBase": "/store/mc",
            "OutputDatasets": [
                "/Primary/Era-Proc-v1/AODSIM",
            ],
        }
        result = derive_merged_lfn_bases(request_data)
        assert len(result) == 1
        assert result[0]["dataset_name"] == "/Primary/Era-Proc-v1/AODSIM"

    def test_default_merged_lfn_base(self):
        """Missing MergedLFNBase defaults to /store/mc."""
        request_data = {
            "OutputDatasets": ["/Primary/Era-Proc-v1/TIER"],
        }
        result = derive_merged_lfn_bases(request_data)
        assert len(result) == 1
        assert result[0]["merged_lfn_base"].startswith("/store/mc/")

    def test_unparseable_dataset_skipped(self):
        """Unparseable dataset names are skipped with a warning."""
        request_data = {
            "OutputDatasets": [
                "/Good/Era-Proc-v1/TIER",
                "bad-no-slashes",
            ],
        }
        result = derive_merged_lfn_bases(request_data)
        assert len(result) == 1
        assert result[0]["dataset_name"] == "/Good/Era-Proc-v1/TIER"


class TestMergedLfnForGroup:
    def test_correct_block_directory(self):
        """Group index → six-digit block number directory."""
        lfn = merged_lfn_for_group("/store/mc/Era/Primary/TIER/Proc-v1", 0)
        assert lfn == "/store/mc/Era/Primary/TIER/Proc-v1/000000/merged.txt"

    def test_custom_filename(self):
        lfn = merged_lfn_for_group("/store/mc/Era/Primary/TIER/Proc-v1", 42, "output.root")
        assert lfn == "/store/mc/Era/Primary/TIER/Proc-v1/000042/output.root"

    def test_large_group_index(self):
        lfn = merged_lfn_for_group("/base", 999999)
        assert lfn == "/base/999999/merged.txt"


class TestLocalOutputPath:
    def test_store_mc_conversion(self):
        """LFN /store/mc/... → /mnt/shared/store/mc/..."""
        result = local_output_path(
            "/mnt/shared/store",
            "/store/mc/Era/Primary/TIER/Proc-v1/000000/merged.txt",
        )
        assert result == "/mnt/shared/store/mc/Era/Primary/TIER/Proc-v1/000000/merged.txt"

    def test_store_unmerged_conversion(self):
        result = local_output_path(
            "/mnt/shared/store",
            "/store/unmerged/Era/Primary/TIER/Proc-v1",
        )
        assert result == "/mnt/shared/store/unmerged/Era/Primary/TIER/Proc-v1"
