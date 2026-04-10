#!/usr/bin/env python3
"""
PyInstaller build script for RomM-RetroArch Sync on Windows
This script bundles the GTK4-based application into a standalone .exe
"""

import PyInstaller.__main__
import os
import sys
import shutil
import glob
from pathlib import Path

# Project paths (script is in scripts/, so parent is project root)
PROJECT_ROOT = Path(__file__).parent.parent
SRC_DIR = PROJECT_ROOT / "src"
ASSETS_DIR = PROJECT_ROOT / "assets"
ICON_FILE = ASSETS_DIR / "icons" / "romm_icon.ico"
BUILD_DIR = PROJECT_ROOT / "build"
DIST_DIR = PROJECT_ROOT / "dist"
RUNTIME_HOOK = Path(__file__).parent / "hook-gi-runtime.py"


def find_conda_prefix():
    """Return the active conda environment prefix."""
    prefix = (
        os.environ.get("CONDA_PREFIX")
        or os.environ.get("CONDA")
        or sys.prefix
    )
    return Path(prefix)


def check_dependencies():
    """Verify required build tools are installed"""
    try:
        import PyInstaller
        import gi
        print("Build dependencies verified")
    except ImportError as e:
        print(f"Missing dependency: {e}")
        print("\nInstall with: conda install -c conda-forge gtk4 libadwaita pygobject pyinstaller")
        sys.exit(1)


def prepare_icon():
    """Prepare or create application icon"""
    if not ICON_FILE.exists():
        print("Warning: icon not found at:", ICON_FILE)
        print("   Using default PyInstaller icon")
        return None
    print(f"Icon found: {ICON_FILE}")
    return str(ICON_FILE)


def collect_gtk_native_data():
    """
    Locate and return (src, dest) pairs for GTK4 typelibs and DLLs from the
    active conda / MSYS2 environment so PyInstaller can bundle them.
    """
    prefix = find_conda_prefix()
    datas = []

    # ---- typelib files (required for gi.require_version to work) ----
    # conda-forge layout: <prefix>/Library/lib/girepository-1.0/*.typelib
    # MSYS2 layout:       <prefix>/lib/girepository-1.0/*.typelib
    for candidate in [
        prefix / "Library" / "lib" / "girepository-1.0",
        prefix / "lib" / "girepository-1.0",
    ]:
        if candidate.exists():
            print(f"  Found typelibs at: {candidate}")
            datas.append((str(candidate), "lib/girepository-1.0"))
            break
    else:
        print("WARNING: GTK4 typelib directory not found. The .exe will not show a window.")

    # ---- GDK-Pixbuf loaders ----
    for candidate in [
        prefix / "Library" / "lib" / "gdk-pixbuf-2.0",
        prefix / "lib" / "gdk-pixbuf-2.0",
    ]:
        if candidate.exists():
            datas.append((str(candidate), "lib/gdk-pixbuf-2.0"))
            break

    # ---- GTK4 compiled schemas (required for settings) ----
    for candidate in [
        prefix / "Library" / "share" / "glib-2.0" / "schemas",
        prefix / "share" / "glib-2.0" / "schemas",
    ]:
        if candidate.exists():
            datas.append((str(candidate), "share/glib-2.0/schemas"))
            break

    # ---- GTK4 icons ----
    for candidate in [
        prefix / "Library" / "share" / "icons",
        prefix / "share" / "icons",
    ]:
        if candidate.exists():
            datas.append((str(candidate), "share/icons"))
            break

    return datas


def collect_gtk_binaries():
    """
    Return (src, dest) pairs for GTK4 native DLLs so they land next to the
    executable and Windows can load them.
    """
    prefix = find_conda_prefix()
    binaries = []

    for bin_dir in [
        prefix / "Library" / "bin",
        prefix / "bin",
    ]:
        if not bin_dir.exists():
            continue
        # Collect DLLs for GTK4 and its core dependencies
        patterns = [
            "libgtk-4*.dll",
            "libgdk_pixbuf*.dll",
            "libglib*.dll",
            "libgobject*.dll",
            "libgio*.dll",
            "libgmodule*.dll",
            "libgthread*.dll",
            "libcairo*.dll",
            "libpango*.dll",
            "libharfbuzz*.dll",
            "libfontconfig*.dll",
            "libfreetype*.dll",
            "libpixman*.dll",
            "libffi*.dll",
            "libintl*.dll",
            "libpcre*.dll",
            "libepoxy*.dll",
            "libadwaita*.dll",
            "libgraphene*.dll",
        ]
        for pattern in patterns:
            for dll in glob.glob(str(bin_dir / pattern)):
                binaries.append((dll, "bin"))
        if binaries:
            print(f"  Collected {len(binaries)} GTK DLLs from {bin_dir}")
            break

    return binaries


def build_exe(icon_path):
    """Build the executable using PyInstaller"""

    hidden_imports = [
        "gi",
        "gi.repository",
        "gi.repository.Gtk",
        "gi.repository.GLib",
        "gi.repository.Gio",
        "gi.repository.GObject",
        "gi.repository.Adw",
        "gi.repository.GdkPixbuf",
        "gi.repository.Pango",
        "gi.repository.Cairo",
        "requests",
        "watchdog",
        "watchdog.observers",
        "watchdog.events",
        "cryptography",
        "PIL",
        "PIL.Image",
        "psutil",
    ]

    # Collect native GTK data / DLLs
    print("\nCollecting GTK4 native files...")
    gtk_datas = collect_gtk_native_data()
    gtk_bins = collect_gtk_binaries()

    # Application data files
    app_datas = [
        (str(ASSETS_DIR / "icons"), "assets/icons"),
        (str(PROJECT_ROOT / "romm_platform_slugs.json"), "."),
        (str(PROJECT_ROOT / "requirements.txt"), "."),
    ]

    all_datas = app_datas + gtk_datas

    # Build arguments
    args = [
        str(SRC_DIR / "romm_sync_app.py"),
        "--name=RomM-RetroArch-Sync",
        f"--distpath={DIST_DIR}",
        f"--workpath={BUILD_DIR}/build",
        f"--specpath={BUILD_DIR}",
        "--onefile",
        "--windowed",
        "--noconfirm",
        f"--runtime-hook={RUNTIME_HOOK}",
    ]

    if icon_path:
        args.append(f"--icon={icon_path}")

    for imp in hidden_imports:
        args.append(f"--hidden-import={imp}")

    for src, dest in all_datas:
        args.append(f"--add-data={src}{os.pathsep}{dest}")

    for src, dest in gtk_bins:
        args.append(f"--add-binary={src}{os.pathsep}{dest}")

    print("\nBuilding executable...")
    print()

    try:
        PyInstaller.__main__.run(args)
        print("\nBuild successful")
    except Exception as e:
        print(f"\nBuild failed: {e}")
        sys.exit(1)

def post_build_info():
    """Display post-build information"""
    exe_path = DIST_DIR / "RomM-RetroArch-Sync.exe"
    
    if exe_path.exists():
        size_mb = exe_path.stat().st_size / (1024 * 1024)
        print(f"\nExecutable: {exe_path}")
        print(f"   Size: {size_mb:.2f} MB")
        print(f"\nTo run the application:")
        print(f"   {exe_path}")
        print(f"\nTo create an installer:")
        print(f"   makensis scripts/installer.nsi")
    else:
        print(f"\nWarning: executable not found at {exe_path}")

def main():
    """Main build process"""
    print("RomM-RetroArch Sync - Windows Build")
    print("=" * 50)
    print()
    
    # Verify environment
    check_dependencies()
    print()
    
    # Prepare assets
    icon = prepare_icon()
    print()
    
    # Build executable
    build_exe(icon)
    print()
    
    # Display results
    post_build_info()

if __name__ == "__main__":
    main()
