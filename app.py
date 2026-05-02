from __future__ import annotations

from latch.web import app

# Compatibility exports for older tests and local scripts that imported helpers from app.py.
from latch.auth import *  # noqa: F401,F403
from latch.config import *  # noqa: F401,F403
from latch.events import *  # noqa: F401,F403
from latch.modules import *  # noqa: F401,F403
from latch.plugin_registry import *  # noqa: F401,F403
from latch.runtime import *  # noqa: F401,F403
from latch.scheduler import *  # noqa: F401,F403
from latch.utils import *  # noqa: F401,F403


if __name__ == "__main__":
    app.run(debug=True, threaded=True)
