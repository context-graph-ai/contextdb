//! Explicit, caller-owned calibration fixtures observe real production runs.
use super::*;
pub type NativeIvfCalibrationAnswerForTest =
    Box<dyn Fn(usize, u32, &[u64]) -> Vec<u64> + Send + Sync>;
struct Answers {
    owner: usize,
    route: [u8; 32],
    answer: NativeIvfCalibrationAnswerForTest,
}
#[derive(Clone, Copy)]
struct Setting {
    owner: usize,
    route: [u8; 32],
    probes: u32,
    factor: u32,
}
thread_local! {
    static ANSWERS:RefCell<Option<Answers>>=const{RefCell::new(None)};
    static SETTING:Cell<Option<Setting>>=const{Cell::new(None)};
}
pub struct NativeIvfCalibrationAnswersForTest {
    previous: Option<Answers>,
}
impl Drop for NativeIvfCalibrationAnswersForTest {
    fn drop(&mut self) {
        let previous = self.previous.take();
        ANSWERS.with(|slot| *slot.borrow_mut() = previous);
    }
}
pub struct NativeIvfCalibrationSettingForTest {
    previous: Option<Setting>,
}
impl Drop for NativeIvfCalibrationSettingForTest {
    fn drop(&mut self) {
        SETTING.with(|slot| slot.set(self.previous));
    }
}
pub(crate) fn arm_answers(
    owner: usize,
    route: [u8; 32],
    answer: NativeIvfCalibrationAnswerForTest,
) -> NativeIvfCalibrationAnswersForTest {
    NativeIvfCalibrationAnswersForTest {
        previous: ANSWERS.with(|slot| {
            slot.replace(Some(Answers {
                owner,
                route,
                answer,
            }))
        }),
    }
}
pub(crate) fn arm_setting(
    owner: usize,
    route: [u8; 32],
    probes: u32,
    factor: u32,
) -> NativeIvfCalibrationSettingForTest {
    NativeIvfCalibrationSettingForTest {
        previous: SETTING.with(|slot| {
            slot.replace(Some(Setting {
                owner,
                route,
                probes,
                factor,
            }))
        }),
    }
}
pub(crate) fn answers_selected(owner: usize, route: [u8; 32]) -> bool {
    ANSWERS.with(|slot| {
        slot.borrow()
            .as_ref()
            .is_some_and(|selected| selected.owner == owner && selected.route == route)
    })
}
pub(crate) fn rewrite(
    owner: usize,
    route: [u8; 32],
    sample: usize,
    depth: u32,
    exact: &[u64],
    output: &mut [u64],
) -> Option<usize> {
    ANSWERS.with(|slot| {
        let selected = slot.borrow();
        let selected = selected
            .as_ref()
            .filter(|selected| selected.owner == owner && selected.route == route)?;
        let result = (selected.answer)(sample, depth, exact);
        let n = result.len().min(output.len());
        output[..n].copy_from_slice(&result[..n]);
        Some(n)
    })
}
pub(crate) fn setting(owner: usize, route: [u8; 32]) -> Option<(u32, u32)> {
    SETTING.with(|slot| {
        slot.get()
            .filter(|selected| selected.owner == owner && selected.route == route)
            .map(|selected| (selected.probes, selected.factor))
    })
}
