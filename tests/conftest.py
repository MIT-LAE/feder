from datetime import datetime
import string

from hypothesis import strategies as st

from feder.common.utils import milli
from feder.common.models import Point


EARLIEST_TIME = int(datetime(2000, 1, 1).timestamp())
LATEST_TIME = int(datetime(2030, 1, 1).timestamp())


point_strategy = st.builds(
    Point,
    time=st.integers(min_value=EARLIEST_TIME, max_value=LATEST_TIME).map(datetime.fromtimestamp),
    lon=st.floats(min_value=-180.0, max_value=180.0).map(milli),
    lat=st.floats(min_value=-90.0, max_value=90.0).map(milli),
    alt=st.floats(min_value=-5000.0, max_value=100000.0).map(milli),
    alt_gnss=st.none(), heading=st.none(), on_ground=st.booleans()
)

short_string_strategy = st.text(
    min_size=10, max_size=255, alphabet=st.characters(codec='ascii')
)

airport_strategy=st.one_of(
    st.none(),
    st.text(min_size=3, max_size=4, alphabet=string.ascii_uppercase)
)
