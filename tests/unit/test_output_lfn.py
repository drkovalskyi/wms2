"""Unit tests for LFN derivation helpers."""

from wms2.core.output_lfn import (
    derive_merged_lfn_bases,
    lfn_to_pfn,
    local_output_path,
    merged_lfn_for_group,
    unmerged_lfn_for_group,
)


class TestLfnToPfn:
    def test_store_mc(self):
        """LFN /store/mc/... → /mnt/shared/store/mc/..."""
        result = lfn_to_pfn(
            "/mnt/shared",
            "/store/mc/Era/Primary/TIER/Proc-v1/000000/merged.root",
        )
        assert result == "/mnt/shared/store/mc/Era/Primary/TIER/Proc-v1/000000/merged.root"

    def test_store_unmerged(self):
        result = lfn_to_pfn(
            "/mnt/shared",
            "/store/unmerged/Era/Primary/TIER/Proc-v1",
        )
        assert result == "/mnt/shared/store/unmerged/Era/Primary/TIER/Proc-v1"

    def test_no_double_slash(self):
        """Prefix without trailing slash + LFN with leading slash → no double slash."""
        result = lfn_to_pfn("/mnt/shared", "/store/mc/test")
        assert "//" not in result

    def test_prefix_with_trailing_slash(self):
        result = lfn_to_pfn("/mnt/shared/", "/store/mc/test")
        assert result == "/mnt/shared/store/mc/test"

    def test_backward_compat_alias(self):
        """local_output_path is an alias for lfn_to_pfn."""
        assert local_output_path("/mnt/shared", "/store/mc/test") == \
               lfn_to_pfn("/mnt/shared", "/store/mc/test")


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

    def test_unmerged_lfn_base_derived(self):
        """Each result includes unmerged_lfn_base derived from UnmergedLFNBase."""
        request_data = {
            "MergedLFNBase": "/store/mc",
            "UnmergedLFNBase": "/store/unmerged",
            "OutputDatasets": [
                "/Primary/Era-Proc-v1/AODSIM",
            ],
        }
        result = derive_merged_lfn_bases(request_data)
        assert len(result) == 1
        assert result[0]["unmerged_lfn_base"] == (
            "/store/unmerged/Era/Primary/AODSIM/Proc-v1"
        )
        assert result[0]["merged_lfn_base"] == (
            "/store/mc/Era/Primary/AODSIM/Proc-v1"
        )

    def test_default_unmerged_lfn_base(self):
        """Missing UnmergedLFNBase defaults to /store/unmerged."""
        request_data = {
            "OutputDatasets": ["/Primary/Era-Proc-v1/TIER"],
        }
        result = derive_merged_lfn_bases(request_data)
        assert len(result) == 1
        assert result[0]["unmerged_lfn_base"].startswith("/store/unmerged/")

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


class TestUnmergedLfnForGroup:
    def test_directory_only(self):
        """Without filename, returns directory path."""
        result = unmerged_lfn_for_group("/store/unmerged/Era/Primary/TIER/Proc-v1", 0)
        assert result == "/store/unmerged/Era/Primary/TIER/Proc-v1/000000"

    def test_with_filename(self):
        result = unmerged_lfn_for_group("/store/unmerged/Era/Primary/TIER/Proc-v1", 3, "file.root")
        assert result == "/store/unmerged/Era/Primary/TIER/Proc-v1/000003/file.root"

    def test_large_group_index(self):
        result = unmerged_lfn_for_group("/base", 999999)
        assert result == "/base/999999"
