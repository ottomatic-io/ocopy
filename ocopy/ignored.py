"""Basenames ignored by copy and post-backup checks (same rule for files and directories at each level)."""

from ascmhl.__version__ import ascmhl_folder_name

ignored_paths = frozenset(
    {
        ".DS_Store",
        ".ocopy-checkpoint",
        ".DocumentRevisions-V100",
        ".Spotlight-V100",
        ".Spotlight",
        ".TemporaryItems",
        ".Trashes",
        ".VolumeIcon.icns",
        "._.TemporaryItems",
        "._.Trashes",
        ".com.apple.timemachine.donotpresent",
        ".fseventsd",
        "System Volume Information",
        "Backups.backupdb",
        ascmhl_folder_name,
        "SONYCARD.IND",
        "SDINFO.TXT",
        ".SD_PROJECT",
    }
)


def is_ignored_basename(name: str) -> bool:
    return name in ignored_paths
