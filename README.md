# FileSweep

<p align="center">
  <img src="./img/logo.png" width="500px" alt="FileSweep screenshot" />
</p>

FileSweep is a cross-platform utility for managing duplicate and unwanted files in user directories. Designed to be run manually or scheduled via cron or Task Scheduler, it helps keep your file collections organized and free of duplicates and clutter.

## Features

- Easily configurable via a single YAML file
- Per-directory priorities and rules
- Detects duplicate files across multiple directories, respecting user-set priorities.
- Supports configurable policies: keep, prompt, hardlink, trash, delete, discard, erase
- Moves files to the recycle bin/trash using [send2trash](https://pypi.org/project/Send2Trash/)
- Minimizes file reads using cached file metadata and hashes
- Cross-platform: works on Windows, macOS, and Linux
- Exclude or include files by name, extension, size, or modification date
- Can be run manually or scheduled for automatic cleanup

## Installation

1. Clone the repository:

    ```sh
    git clone https://github.com/ramsteak/FileSweep.git
    cd filesweep
    ```

2. Install dependencies:

    ```sh
    pip install send2trash
    ```

## Configuration

Move `filesweep.yaml` to your user folder (e.g., `~/.filesweep/config.yaml` on Linux/macOS or `%USERPROFILE%\.filesweep\config.yaml` on Windows).
Edit the file to specify directories, policies, and file type rules.

### Directory Options

- `path`: Directory to scan
- `priority`: Higher priority directories keep their files in case of duplicates
- `subdirs`: If false, only scan the top-level directory
- `policy`: Action to take on duplicates or matches (see below)
- `rename`: If true, handles duplicate filenames by keeping the oldest
- `skip_subdirs`: A list of all directory names to skip when discovering files

### Policy Options

- `keep`: Never delete files in this directory
- `prompt`: Ask before deleting
- `hardlink`: Replace low priority duplicates with hardlinks
- `trash`: Move duplicates to recycle bin/trash
- `delete`: Always delete duplicates, keep highest priority
- `discard!`: Move matching files (not only duplicates) to trash
- `erase!`: Delete matching files (not only duplicates)

### File Type Rules

- `exclude`: Patterns, names, or regexes to skip files
- `include`: Size and modification date filters

## Usage

Run FileSweep from the command line:

```sh
python -m filesweep
```

Or schedule it using your system’s scheduler (cron, Task Scheduler, etc.).

## Requirements

- Python 3.11+
- [send2trash](https://pypi.org/project/Send2Trash/)

## License

This project is licensed under the [MIT License](LICENSE).

## Contributing

Pull requests and suggestions are welcome!  
If you encounter issues or have feature requests, please open an issue on the GitHub repository.

## FAQ

**Q: Will FileSweep delete files automatically?**  
A: Only if the policy is set to `delete`, `erase!`, or similar. Use `prompt` for confirmation before deletion.

**Q: Can I run FileSweep on a schedule?**  
A: Yes, you can use cron (Linux/macOS) or Task Scheduler (Windows) to automate runs. After the first run and if cache is set, only check for new or deleted files, without rehashing all files.

**Q: Does FileSweep support network drives or external disks?**  
A: Yes, as long as Python can access the path.

**Q: How do I exclude certain file types or folders?**  
A: Use the `exclude` section in `filesweep.yaml` to specify patterns or names, or `skip_subdirs` in the directory config.

---

Feel free to suggest improvements or ask for additional features!
