from subprocess import CalledProcessError
from unittest.mock import patch

import pytest

from processors.base import get_arch, get_exports, get_extension, get_imports

DUMMY_IMPORTS_RESPONSE1 = (
    """Contents of /tmp/00Nb1Q3mxXNb6fvAp3SrscnVWACdUwpM.exe: 118784 bytes

Done dumping /tmp/00Nb1Q3mxXNb6fvAp3SrscnVWACdUwpM.exe"""
)

DUMMY_IMPORTS_RESPONSE2 = (
    """Contents of /tmp/00Nb1Q3mxXNb6fvAp3SrscnVWACdUwpM.exe: 118784 bytes

Import Table size: 00000050
    offset 00010790 KERNEL32.dll
    Hint/Name Table: 00010834
    TimeDateStamp:   00000000 (Thu Jan  1 00:00:00 1970)
    ForwarderChain:  00000000
    First thunk RVA: 0000F054
    Thunk    Ordn  Name
    0000f054   440  IsBadWritePtr

    offset 000107a4 USER32.dll
    Hint/Name Table: 00010914
    TimeDateStamp:   00000000 (Thu Jan  1 00:00:00 1970)
    ForwarderChain:  00000000
    First thunk RVA: 0000F134
    Thunk    Ordn  Name
    0000f134   132  DefWindowProcA

    offset 000107b8 GDI32.dll
    Hint/Name Table: 000107E0
    TimeDateStamp:   00000000 (Thu Jan  1 00:00:00 1970)
    ForwarderChain:  00000000
    First thunk RVA: 0000F000
    Thunk    Ordn  Name
    0000f000   428  RealizePalette
    0000f04c   342  GetPixel


Done dumping /tmp/00Nb1Q3mxXNb6fvAp3SrscnVWACdUwpM.exe"""
)

DUMMY_IMPORTS_RESPONSE3 = (
    """Contents of 00ELuByj9iSRf5Rx11Ypl15N6kS2FXmW.dll: 3919 bytes

Can't get a suitable file signature, aborting"""
)

DUMMY_EXPORTS_RESPONSE1 = (
    """Contents of /tmp/00Nb1Q3mxXNb6fvAp3SrscnVWACdUwpM.exe: 118784 bytes

Done dumping /tmp/00Nb1Q3mxXNb6fvAp3SrscnVWACdUwpM.exe"""
)

DUMMY_EXPORTS_RESPONSE2 = (
    """Contents of 31g2WybCNZHwEWclpbP0ZjjebZ8WmDtJ.dll: 14848 bytes

Exports table:

  Name:            SensApi.dll
  Characteristics: 00000000
  TimeDateStamp:   B87025F2 Sat Jan 21 07:44:50 2068
  Version:         0.00
  Ordinal base:    1
  # of functions:  3
  # of Names:      3
Addresses of functions: 00003DA8
Addresses of name ordinals: 00003DC0
Addresses of names: 00003DB4

  Entry Pt  Ordn  Name
  00001110     1 IsDestinationReachableA
  00001110     2 IsDestinationReachableW
  00001010     3 IsNetworkAlive

Done dumping 31g2WybCNZHwEWclpbP0ZjjebZ8WmDtJ.dll"""
)

DUMMY_EXPORTS_RESPONSE3 = (
    """Contents of 00ELuByj9iSRf5Rx11Ypl15N6kS2FXmW.dll: 3919 bytes

Can't get a suitable file signature, aborting"""
)

DUMMY_ARCH_RESPONSE = (
    """
017xEB9ZOAAG3VdYpTc0kBPzlh40cPab.exe:     file format pei-i386
architecture: i386, flags 0x0000010a:
EXEC_P, HAS_DEBUG, D_PAGED
start address 0x00401a30

"""
)
@pytest.mark.parametrize('mocked_stdout, expected_result', [
    (
        "", []
    ),
    (
        CalledProcessError(returncode=1, cmd="dummy_cmd"), None
    ),
    (
        DUMMY_IMPORTS_RESPONSE1, []
    ),
    (
        DUMMY_IMPORTS_RESPONSE2, ["KERNEL32.dll", "USER32.dll", "GDI32.dll"]
    ),
    (
        DUMMY_IMPORTS_RESPONSE3, None
    )
])
def test_get_imports(mocked_stdout, expected_result):
    with patch('processors.base.run_cmd') as mocked_run_cmd:
        mocked_run_cmd.side_effect = [mocked_stdout]
        assert get_imports("dummy_path") == expected_result


@pytest.mark.parametrize('mocked_stdout, expected_result', [
    (
        "", []
    ),
    (
        CalledProcessError(returncode=1, cmd="dummy_cmd"), None
    ),
    (
        DUMMY_EXPORTS_RESPONSE1, []
    ),
    (
        DUMMY_EXPORTS_RESPONSE2, ["SensApi.dll"]
    ),
    (
        DUMMY_EXPORTS_RESPONSE3, None
    )
])
def test_get_exports(mocked_stdout, expected_result):
    with patch('processors.base.run_cmd') as mocked_run_cmd:
        mocked_run_cmd.side_effect = [mocked_stdout]
        assert get_exports("dummy_path") == expected_result


@pytest.mark.parametrize('mocked_stdout, expected_result', [
    (
        "", ""
    ),
    (
        CalledProcessError(returncode=1, cmd="dummy_cmd"), None
    ),
    (
        DUMMY_ARCH_RESPONSE, "i386"
    )
])
def test_get_arch(mocked_stdout, expected_result):
    with patch('processors.base.run_cmd') as mocked_run_cmd:
        mocked_run_cmd.side_effect = [mocked_stdout]
        assert get_arch("dummy_path") == expected_result


@pytest.mark.parametrize('file_path, expected_result', [
    (
        "04OedY8viaNBFEDv26n08fU2GnfxxmFv.dll", "dll"
    ),
    (
        "04wwBoUBnPWiKg5eAH2YNtiy6XGjZV3X.exe", "exe"
    ),
    (
        "", ""
    ),
    (
        "04OedY8viaNBFEDv26n08fU2GnfxxmFv", ""
    )
])
def test_get_extension(file_path, expected_result):
    assert get_extension(file_path) == expected_result
