"""
PyInstaller runtime hook for PyGObject / GTK4 on Windows.

This hook runs inside the bundled .exe at startup, before any application
code runs. It points GI_TYPELIB_PATH and GDK_PIXBUF_MODULEDIR at the
directories we bundled alongside the executable so that gi.require_version
can find the Gtk namespace.
"""

import os
import sys

_base = getattr(sys, "_MEIPASS", os.path.dirname(os.path.abspath(__file__)))

# Tell GObject introspection where to find typelib files
os.environ["GI_TYPELIB_PATH"] = os.path.join(_base, "lib", "girepository-1.0")

# Add the bundled lib dir to PATH so Windows can find the GTK DLLs
_lib_dir = os.path.join(_base, "lib")
_bin_dir = os.path.join(_base, "bin")
os.environ["PATH"] = _bin_dir + os.pathsep + _lib_dir + os.pathsep + os.environ.get("PATH", "")

# GDK pixbuf loaders (needed for icons/images)
os.environ.setdefault(
    "GDK_PIXBUF_MODULEDIR",
    os.path.join(_base, "lib", "gdk-pixbuf-2.0", "2.10.0", "loaders"),
)
