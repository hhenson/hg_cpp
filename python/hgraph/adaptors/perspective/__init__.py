"""Optional Perspective table publishing.

Importing this package does not import ``perspective``. The client is loaded
only when a real table or web endpoint is requested; tests and applications
may inject a compatible client through :class:`PerspectiveTablesManager`.
"""

from ._perspective import *
from ._perspective_publish import *
from ._perspective_adaptor import *
