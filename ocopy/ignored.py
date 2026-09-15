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


# AppleDouble companions (``._<name>``) hold a file's extended attributes on
# filesystems without native support: FAT/exFAT cards and SMB shares without
# named streams. macOS creates and rewrites them itself, so they are not media,
# and one on a share is rewritten as soon as the real file's attributes change,
# which makes a later ``skip_existing`` run see it as a conflicting file.
appledouble_prefix = "._"

# ``ignored_paths`` plus the AppleDouble rule, as gitwildmatch patterns for ASC MHL.
ignore_patterns = (*sorted(ignored_paths), f"{appledouble_prefix}*")


def is_ignored_basename(name: str) -> bool:
    return name in ignored_paths or name.startswith(appledouble_prefix)
