// =============================================================================
// INTERVIEW PROBLEM 4: Biomarker Alert Monitor
// Difficulty: Senior Software Engineer | Estimated time: 45 min
// =============================================================================
//
// CONTEXT
// -------
// A health-tech platform delivers remote clinical care for type 2 diabetes
// reversal. Patients use connected glucometers and ketone meters that sync
// readings into the app several times per day. The care team dashboard needs
// to surface patients whose numbers have been out of target range for multiple
// consecutive days, so health coaches can prioritize outreach.
//
// You are building the BiomarkerMonitor — the type that processes a stream of
// patient readings and answers questions about trends and outreach priority.
//
// DATA MODEL
// ----------
// BiomarkerReading — PatientID, ReadingType ("glucose"|"ketone"|"weight"),
//                    Value (float64), RecordedOn (string "2006-01-02")
//
// Target ranges (inclusive):
//   Glucose: 70–180 mg/dL   (< 70 = dangerous low, > 180 = hyperglycemia)
//   Ketone:  0.5–3.0 mmol/L (below = not in ketosis, above = monitor)
//   Weight:  no absolute target — never flagged as out-of-range
//
// OutreachEntry — PatientID, ReadingType, ConsecutiveDays, LatestValue
//
// NOTES
// -----
//   - Multiple readings on the same calendar day count as ONE day.
//     A day is "out-of-range" if ANY reading that day is out of range.
//   - All mutable state must be stored in struct fields set in the constructor.
//     Do NOT use package-level variables — they bleed state between instances.
//
// EXAMPLE
// -------
//   readings := []BiomarkerReading{
//       {"alice", "glucose", 195.0, "2024-01-01"},
//       {"alice", "glucose", 202.0, "2024-01-02"},
//       {"alice", "glucose", 188.0, "2024-01-03"},
//   }
//   m := NewBiomarkerMonitor(readings)
//   m.MaxConsecutiveOutOfRangeDays("alice", "glucose")  // -> 3
//   m.GetOutreachList(3)
//   // -> [{PatientID:"alice", ReadingType:"glucose", ConsecutiveDays:3, LatestValue:188.0}]
// =============================================================================

package biomarker

import (
	"errors"
	"sort"
	"time"
)

// GlucoseMin and GlucoseMax define the inclusive target range for glucose (mg/dL).
const GlucoseMin = 70.0
const GlucoseMax = 180.0

// KetoneMin and KetoneMax define the inclusive target range for ketones (mmol/L).
const KetoneMin = 0.5
const KetoneMax = 3.0

// ErrAlreadyExists is returned when creating a resource with a duplicate ID.
var ErrAlreadyExists = errors.New("already exists")

// ErrNotFound is returned when accessing a resource that does not exist.
var ErrNotFound = errors.New("not found")

// BiomarkerReading is a single biomarker measurement from a patient device or
// manual entry. RecordedOn is an ISO date string ("2006-01-02").
type BiomarkerReading struct {
	PatientID   string
	ReadingType string // "glucose" | "ketone" | "weight"
	Value       float64
	RecordedOn  string // ISO date: "2006-01-02"
}

// OutreachEntry describes a patient who needs proactive coach outreach.
type OutreachEntry struct {
	PatientID       string
	ReadingType     string
	ConsecutiveDays int
	LatestValue     float64
}

type OutreachEntryWithTime struct {
	OutreachEntry
	LatestRecordedOn string
}

type BiomarkerMonitor interface {
	// IsOutOfRange returns true if the reading falls outside the target range
	// for its reading type.
	//   - Glucose: outside [70, 180] mg/dL (inclusive)
	//   - Ketone:  outside [0.5, 3.0] mmol/L (inclusive)
	//   - Weight:  never out-of-range (always returns false)
	IsOutOfRange(reading BiomarkerReading) bool

	// MaxConsecutiveOutOfRangeDays returns the length of the longest streak of
	// consecutive calendar days on which the patient had at least one out-of-range
	// reading of the given type.
	//
	// Returns 0 if the patient has no out-of-range readings of that type.
	//
	// "Consecutive" means no gap: Jan 1, Jan 2, Jan 3 is a streak of 3.
	// Jan 1 and Jan 3 (skipping Jan 2) are two separate streaks of 1.
	// Multiple readings on the same day collapse to one day.
	MaxConsecutiveOutOfRangeDays(patientID, readingType string) int

	// GetOutreachList returns a list of patients who need proactive coach
	// outreach because they have been out-of-range for at least
	// minConsecutiveDays in a row.
	//
	// A patient can appear more than once if multiple reading types cross the
	// threshold (e.g., both glucose and ketone streaks).
	//
	// The list is sorted by ConsecutiveDays descending (most urgent first).
	GetOutreachList(minConsecutiveDays int) []OutreachEntry

	AddReading(reading BiomarkerReading) bool
}

func NewBiomarkerMonitor(readings []BiomarkerReading) BiomarkerMonitor {
	obj := &biomarkerMonitor{
		users:          make(map[string]map[string]*OutreachEntryWithTime),
		maxDaysOfUsers: make(map[string]map[string]int),
	}

	for _, r := range readings {
		_ = obj.AddReading(r)
	}
	return obj
}

type biomarkerMonitor struct {
	users          map[string]map[string]*OutreachEntryWithTime // patientID to latest record summary of a readingType
	maxDaysOfUsers map[string]map[string]int                    // tracking the maximum consecutive days of a patientID by readingType
}

func isNextDay(previous, current string) bool {
	const layout = "2006-01-02"

	prevDate, _ := time.Parse(layout, previous)
	currentDate, _ := time.Parse(layout, current)

	return currentDate.Equal(prevDate.AddDate(0, 0, 1))
}

func (b biomarkerMonitor) IsOutOfRange(reading BiomarkerReading) bool {
	//   - Glucose: outside [70, 180] mg/dL (inclusive)
	//   - Ketone:  outside [0.5, 3.0] mmol/L (inclusive)
	//   - Weight:  never out-of-range (always returns false)

	if reading.ReadingType == "glucose" && (reading.Value < 70 || reading.Value > 180) {
		return true
	}

	if reading.ReadingType == "ketone" && (reading.Value < 0.5 || reading.Value > 3.0) {
		return true
	}

	return false
}

func (b biomarkerMonitor) MaxConsecutiveOutOfRangeDays(patientID, readingType string) int {
	if days, exist := b.maxDaysOfUsers[patientID][readingType]; exist {
		return days
	}
	return 0
}

func (b biomarkerMonitor) GetOutreachList(minConsecutiveDays int) []OutreachEntry {
	res := make([]OutreachEntry, 0)

	for _, v := range b.users {
		for t, e := range v {
			if e != nil && e.ConsecutiveDays >= minConsecutiveDays {
				res = append(res, OutreachEntry{
					PatientID:       e.PatientID,
					ReadingType:     t,
					ConsecutiveDays: e.ConsecutiveDays,
					LatestValue:     e.LatestValue,
				})
			}
		}
	}

	// Sort by consecutiveDays descending order
	sort.Slice(res, func(i, j int) bool {
		return res[i].ConsecutiveDays > res[j].ConsecutiveDays
	})

	return res
}

func (b *biomarkerMonitor) AddReading(r BiomarkerReading) bool {
	if _, exist := b.users[r.PatientID]; !exist {
		b.users[r.PatientID] = make(map[string]*OutreachEntryWithTime)
	}

	if _, exist := b.maxDaysOfUsers[r.PatientID]; !exist {
		b.maxDaysOfUsers[r.PatientID] = make(map[string]int)
	}

	latestAlert := b.users[r.PatientID][r.ReadingType]
	if latestAlert != nil && latestAlert.LatestRecordedOn == r.RecordedOn && (latestAlert.LatestValue >= r.Value-0.5 && latestAlert.LatestValue <= r.Value+0.5) {
		return false // not update
	}

	if b.IsOutOfRange(r) {
		// If this is the first day or it break the consecutive
		if latestAlert != nil {
			if isNextDay(latestAlert.LatestRecordedOn, r.RecordedOn) {
				latestAlert.ConsecutiveDays += 1
				latestAlert.LatestValue = r.Value
				latestAlert.LatestRecordedOn = r.RecordedOn
			} else {
				latestAlert.LatestValue = r.Value
			}
		} else {
			b.users[r.PatientID][r.ReadingType] = &OutreachEntryWithTime{
				OutreachEntry: OutreachEntry{
					PatientID:       r.PatientID,
					ReadingType:     r.ReadingType,
					ConsecutiveDays: 1,
					LatestValue:     r.Value,
				},
				LatestRecordedOn: r.RecordedOn,
			}
		}
		// Update the maxDaysOfUsers
		b.maxDaysOfUsers[r.PatientID][r.ReadingType] = b.users[r.PatientID][r.ReadingType].ConsecutiveDays
	} else {
		b.maxDaysOfUsers[r.PatientID][r.ReadingType] = 0
		b.users[r.PatientID][r.ReadingType] = nil
	}
	return true
}
