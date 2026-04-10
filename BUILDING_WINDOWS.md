# Building RomM-RetroArch Sync for Windows

This guide explains how to build a standalone Windows executable (.exe) and optional installer for RomM-RetroArch Sync.

## Prerequisites

You need one of these environments. Choose the one that best fits your workflow:

### Option A: Anaconda/Miniconda (Easiest)

**Install with winget (recommended):**
```powershell
winget install Anaconda.Miniconda3
```

**Or download manually:**
1. Download [Miniconda](https://docs.conda.io/projects/miniconda/en/latest/)
2. Run the installer

Then skip to "Step 1: Install Dependencies"

### Option B: MSYS2 + pip (Recommended if you prefer native pip)
1. Download [MSYS2](https://www.msys2.org/)
2. Install it
3. Use MinGW-w64-x86_64 packages for GTK4, pip for Python dependencies
4. Skip to "Step 1: Install Dependencies"

### Option C: GTK4 Pre-built + pip (Minimal install)
1. Download [GTK4 pre-built binaries](https://www.gtk.org/docs/installations/windows/)
2. Extract to folder and add to PATH
3. Use pip for Python dependencies
4. Skip to "Step 1: Install Dependencies"

### Option D: Native Windows Python (Advanced)
Requires manual GTK4 installation from source or pre-built binaries and careful PATH setup

## Step 1: Install Dependencies

### Using Anaconda/Miniconda (Easiest)

```bash
# Create a new conda environment
conda create -n romm-build python=3.10 -y

# Activate it
conda activate romm-build

# Install GTK4 and dependencies via conda-forge
conda install -c conda-forge gtk4 libadwaita pygobject pyinstaller pkg-config -y

# Install build requirements
pip install -r requirements-build.txt
```

### Using MSYS2 + pip

```bash
# Launch MSYS2 MinGW 64-bit terminal
pacman -Syu

# Install GTK4 and dependencies via pacman
pacman -S mingw-w64-x86_64-gtk4 \
          mingw-w64-x86_64-libadwaita \
          mingw-w64-x86_64-python \
          mingw-w64-x86_64-python-pip \
          mingw-w64-x86_64-python-gobject \
          mingw-w64-x86_64-gcc

# Install Python build requirements via pip
pip install -r requirements-build.txt
```

### Using GTK4 Pre-built + pip (Minimal)

```bash
# 1. Download GTK4 from: https://www.gtk.org/docs/installations/windows/
#    (Look for "Pre-built binaries" or "Nightly builds")

# 2. Extract GTK4 to a folder, e.g.: C:\gtk4-runtime

# 3. Add GTK4 bin folder to your PATH:
#    - Open PowerShell or Command Prompt
#    - Or edit System Environment Variables manually

# Option A: Temporary (this session only)
$env:PATH = "C:\gtk4-runtime\bin;" + $env:PATH

# Option B: Permanent (restart terminal after)
[Environment]::SetEnvironmentVariable("PATH", "C:\gtk4-runtime\bin;$env:PATH", [EnvironmentVariableTarget]::User)

# 4. Verify GTK4 is in PATH:
pkg-config --modversion gtk+-4.0

# 5. Install Python build requirements
pip install -r requirements-build.txt
```

### Using Native Windows Python (Manual setup)

For advanced users: Install GTK4 from [gtk.org](https://www.gtk.org/docs/installations/windows/), 
add `/bin` folder to PATH, then run `pip install -r requirements-build.txt`


## Step 2: Build the Executable

### From Anaconda environment:

```bash
# Activate the environment
conda activate romm-build

# Run the build script
python scripts/build_windows.py

# The .exe will be created in: dist/RomM-RetroArch-Sync.exe
```

### From MSYS2:

```bash
# In your MSYS2 MinGW 64-bit terminal
cd /c/Users/Max/Repos/romm-retroarch-sync

python scripts/build_windows.py

# The .exe will be created in: dist/RomM-RetroArch-Sync.exe
```

### From native pip setup (GTK4 pre-built):

```bash
# Make sure GTK4 is in your PATH (from Step 1)
# Then run:
python scripts/build_windows.py

# The .exe will be created in: dist/RomM-RetroArch-Sync.exe
```

## Step 3 (Optional): Create an Installer

To create an MSI-like installer (.exe), install NSIS and run:

### Install NSIS
- Download from [nsis.sourceforge.io](https://nsis.sourceforge.io/)
- Run the installer

### Build the installer

```bash
# Make sure the .exe was built in step 2
makensis scripts/installer.nsi

# The installer will be: dist/RomM-RetroArch-Sync-v1.5.0-installer.exe
```

## Troubleshooting

### Issue: "GTK4 not found" or "GtkSourcView-6 not found"

**Solutions by setup type**:

- **Anaconda**: 
  - Verify conda environment is activated: `conda activate romm-build`
  - Reinstall GTK: `conda install -c conda-forge gtk4 libadwaita pygobject pyinstaller pkg-config -y`

- **MSYS2**: 
  - Use MinGW 64-bit terminal, NOT MSYS2 terminal
  - Verify GTK is installed: `pacman -Qs gtk4`
  - If missing, reinstall: `pacman -S mingw-w64-x86_64-gtk4`

- **GTK4 Pre-built + pip**:
  - Verify GTK4 bin folder is in PATH: `echo $env:PATH` (PowerShell) or `echo %PATH%` (cmd)
  - Test GTK4 directly: `pkg-config --modversion gtk+-4.0`
  - If not found, add manually in PowerShell: `$env:PATH = "C:\gtk4-runtime\bin;" + $env:PATH`
  - Or set permanently via System Environment Variables

### Issue: "PyGObject import error"

**Solution**:
```bash
# Install PyGObject from conda or MSYS2, not pip, on Windows
conda install -c conda-forge pygobject -y
```

### Issue: "Icon file not found"

**Solution**: The build will proceed without an icon. To add one:
1. Place an .ico file at: `assets/icons/romm_icon.ico`
2. Rebuild with `python build_windows.py`

### Issue: Large .exe file (>500MB)

This is normal for GTK4 applications. The executable includes:
- Python runtime
- GTK4 libraries
- All dependencies

To reduce size for distribution, you can customize PyInstaller settings in `build_windows.py`.

## Output Files

After building:

- **Executable**: `dist/RomM-RetroArch-Sync.exe` (~300-500MB)
- **Installer**: `dist/RomM-RetroArch-Sync-v1.5.0-installer.exe` (after NSIS)

## Running the Application

### Direct execution:
```bash
dist/RomM-RetroArch-Sync.exe
```

### From installer:
1. Run the installer .exe
2. Follow the wizard
3. Launch from Start Menu or Desktop shortcut

## Developer Notes

### Modifying the build:

- **Change app name**: Edit `build_windows.py`, line with `--name=`
- **Add files to bundle**: Add to `datas` list in `build_windows.py`
- **Change installer output**: Edit `OutFile` in `installer.nsi`
- **Update version**: Edit version strings in both scripts

### Debugging GTK issues:

```bash
# Run with debug output
set GTK_DEBUG=all
python dist/RomM-RetroArch-Sync.exe

# Or from source during development
python src/romm_sync_app.py
```

## References

- [PyInstaller Documentation](https://pyinstaller.readthedocs.io/)
- [GTK4 on Windows](https://docs.gtk.org/gtk4/building.html)
- [NSIS Documentation](https://nsis.sourceforge.io/Docs/)
- [Conda-forge GTK packages](https://anaconda.org/conda-forge/gtk)
