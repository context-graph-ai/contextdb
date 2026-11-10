use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::{Clock, now_millis};

struct At(SystemTime);

impl Clock for At {
    fn now(&self) -> SystemTime {
        self.0
    }
}

#[test]
fn the_time_of_day_is_milliseconds_since_1970_on_either_side_of_it() {
    let after = At(UNIX_EPOCH + Duration::from_millis(1_700_000_000_123));
    assert_eq!(now_millis(&after), 1_700_000_000_123);
    let before = At(UNIX_EPOCH - Duration::from_millis(1_500));
    assert_eq!(now_millis(&before), -1_500);
}
