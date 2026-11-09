//! A refused covered-journal retry defers only its own build. A later search
//! must still prove every partition's frontier before publishing an answer.
