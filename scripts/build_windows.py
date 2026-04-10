#!/usr/bin/env python3
"""
PyInstaller build script for RomM-RetroArch Sync on Windows
This script bundles the GTK4-based application into a standalone .exe
"""

import PyInstaller.__main__
import os
import sys
import shutil
from pathlib import Path

# Project paths (script is in scripts/, so parent is project root)
PROJECT_ROOT = Path(__file__).parent.parent
SRC_DIR = PROJECT_ROOT / "src"
ASSETS_DIR = PROJECT_ROOT / "assets"
ICON_FILE = ASSETS_DIR / "icons" / "romm_icon.ico"
BUILD_DIR = PROJECT_ROOT / "build"
DIST_DIR = PROJECT_ROOT / "dist"

# PyInstaller paths
SPEC_FILE = BUILD_DIR / "romm_sync.spec"

def check_dependencies():
    """Verify required build tools are installed"""
    required = ["pyinstaller", "PyGObject"]
    
    try:
        import PyInstaller
        import gi
        print("✅ Build dependencies verified")
    except ImportError as e:
        print(f"❌ Missing dependency: {e}")
        print("\nInstall with: pip install -r requirements-build.txt")
        sys.exit(1)

def prepare_icon():
    """Prepare or create application icon"""
    if not ICON_FILE.exists():
        print("⚠️  Icon not found at:", ICON_FILE)
        print("   Using default PyInstaller icon")
        return None
    
    print(f"✅ Icon found: {ICON_FILE}")
    return str(ICON_FILE)

def collect_gtk_dependencies():
    """Collect GTK4 runtime dependencies from MSYS2/conda environment"""
    # This assumes GTK4 is installed via conda or available in system PATH
    hidden_imports = [
        "gi",
        "gi.repository",
        "gi.repository.Gtk",
        "gi.repository.GLib",
        "gi.repository.Gio",
        "gi.repository.GObject",
        "gi.repository.Adw",
    ]
    
    return hidden_imports

def build_exe(icon_path):
    """Build the executable using PyInstaller"""
    
    hidden_imports = collect_gtk_dependencies()
    
    # Additional runtime dependencies
    hidden_imports.extend([
        "requests",
        "watchdog",
        "cryptography",
        "PIL",
        "psutil",
    ])
    
    # Data files to include
    datas = [
        (str(ASSETS_DIR / "icons"), "assets/icons"),
        (str(PROJECT_ROOT / "romm_platform_slugs.json"), "."),
    ]
    
    # Build arguments
    args = [
        str(SRC_DIR / "romm_sync_app.py"),
        f"--name=RomM-RetroArch-Sync",
        f"--distpath={DIST_DIR}",
        f"--buildpath={BUILD_DIR}/build",
        f"--specpath={BUILD_DIR}",
        "--onefile",  # Create single executable
        "--windowed",  # No console window
        "--noconfirm",
        "--add-data=requirements.txt:.",
    ]
    
    # Add icon if available
    if icon_path:
        args.append(f"--icon={icon_path}")
    
    # Add hidden imports
    for imp in hidden_imports:
        args.append(f"--hidden-import={imp}")
    
    # Add data files
    for src, dest in datas:
        args.append(f"--add-data={src}{os.pathsep}{dest}")
    
    print("\n🔨 Building executable...")
    print(f"   Command: pyinstaller {' '.join(args)}")
    print()
    
    try:
        PyInstaller.__main__.run(args)
        print("\n✅ Build successful!")
    except Exception as e:
        print(f"\n❌ Build failed: {e}")
        sys.exit(1)

def post_build_info():
    """Display post-build information"""
    exe_path = DIST_DIR / "RomM-RetroArch-Sync.exe"
    
    if exe_path.exists():
        size_mb = exe_path.stat().st_size / (1024 * 1024)
        print(f"\n📦 Executable: {exe_path}")
        print(f"   Size: {size_mb:.2f} MB")
        print(f"\n🚀 To run the application:")
        print(f"   {exe_path}")
        print(f"\n📦 To create an installer:")
        print(f"   makensis scripts/installer.nsi")
    else:
        print(f"\n⚠️  Executable not found at {exe_path}")

def main():
    """Main build process"""
    print("🚀 RomM-RetroArch Sync - Windows Build")
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
