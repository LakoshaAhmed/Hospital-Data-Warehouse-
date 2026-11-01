"""
Generate a large synthetic hospital dataset (the "raw zone" of the warehouse).

It scales up the tables from the Hospital Management Database project:
hospitals, doctors, patients and operations. The raw data is deliberately a
little messy (extra spaces, mixed letter case, duplicate rows, missing values
and free-text notes), so the PySpark ETL has real cleaning work to do.

Usage:
    python src/generate_data.py --operations 5000000 --out data/raw
"""

import argparse
import os
import time

import numpy as np
import pandas as pd

HOSPITALS = [
    ("St. Mary's Medical Center", "Springfield, IL"),
    ("City General Hospital", "Chicago, IL"),
    ("Riverside Health System", "Riverside, CA"),
    ("Northside Community Hospital", "Atlanta, GA"),
    ("Lakewood Medical Center", "Denver, CO"),
    ("Sunrise Regional Hospital", "Miami, FL"),
    ("Valley View Medical Center", "Seattle, WA"),
    ("Coastal Health Institute", "Boston, MA"),
    ("Greenfield Hospital", "Austin, TX"),
    ("Summit Care Medical", "Dallas, TX"),
    ("Maplewood General", "Phoenix, AZ"),
    ("Eastside Medical Center", "Portland, OR"),
    ("Harmony Health Hospital", "Nashville, TN"),
    ("Westbrook Medical", "San Diego, CA"),
    ("Pine Ridge Hospital", "Orlando, FL"),
    ("Clearwater Health Center", "Detroit, MI"),
    ("Mountainview Medical", "Minneapolis, MN"),
    ("Cedarhill Hospital", "Baltimore, MD"),
    ("Brookside Regional", "Sacramento, CA"),
    ("Harborview Medical Center", "Las Vegas, NV"),
]

SPECIALTIES = [
    "Anesthesiology", "Cardiology", "Dermatology", "Emergency Medicine",
    "Endocrinology", "Gastroenterology", "General Surgery", "Hematology",
    "Internal Medicine", "Nephrology", "Neurology", "Oncology", "Ophthalmology",
    "Orthopedics", "Pediatrics", "Psychiatry", "Pulmonology", "Radiology",
    "Rheumatology", "Urology",
]

OPERATIONS = [
    "ACL Reconstruction", "Adenoidectomy", "Angioplasty", "Aortic Valve Replacement",
    "Appendectomy", "Carotid Endarterectomy", "Carpal Tunnel Release",
    "Cataract Surgery", "Cholecystectomy", "Colectomy", "Coronary Bypass Graft",
    "Craniotomy", "Gastric Bypass", "Hernia Repair", "Hip Replacement",
    "Hysterectomy", "Knee Arthroplasty", "Laparoscopic Myomectomy",
    "Liver Resection", "Lung Lobectomy", "Mastectomy", "Nephrectomy",
    "Pancreaticoduodenectomy", "Prostatectomy", "Rotator Cuff Repair",
    "Sleeve Gastrectomy", "Spinal Fusion", "Splenectomy", "Thyroidectomy",
    "Tonsillectomy",
]

FIRST_NAMES = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael", "Linda",
    "David", "Elizabeth", "William", "Barbara", "Richard", "Susan", "Joseph",
    "Jessica", "Thomas", "Sarah", "Charles", "Karen", "Daniel", "Lisa", "Mark",
    "Nancy", "Ahmed", "Fatima", "Omar", "Layla", "Ivan", "Anna", "Dmitry", "Olga",
]
LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Wilson", "Anderson", "Taylor", "Thomas", "Moore",
    "Jackson", "Martin", "Lee", "Walker", "Hall", "Allen", "Young", "King",
    "Wright", "Hassan", "Ali", "Ibrahim", "Ivanov", "Petrov", "Smirnov",
]

# Free-text notes written by doctors. The ETL extracts structured fields from
# them with regular expressions: allergy, blood loss, duration, complication.
ALLERGIES = ["penicillin", "latex", "iodine", "aspirin", "morphine"]
COMPLICATIONS = ["infection", "bleeding", "blood clot", "pneumonia", "wound dehiscence"]
PLAIN_NOTES = [
    "Patient stable post-operation.",
    "Post-op observation required for 48 hours.",
    "Laparoscopic approach used successfully.",
    "Procedure completed without complications.",
    "Patient responded well to anesthesia.",
    "Tissue sample sent for biopsy.",
    "Patient history of hypertension noted.",
    "Follow-up scheduled in two weeks.",
    "ICU admission recommended post-op.",
]


def make_people(rng, n):
    first = rng.choice(FIRST_NAMES, n)
    last = rng.choice(LAST_NAMES, n)
    return first, last


def messy(rng, values, share=0.05):
    """Add extra spaces and random letter case to a share of the strings."""
    s = pd.Series(values, dtype="object")
    idx = rng.random(len(s)) < share
    upper = rng.random(len(s)) < 0.5
    s[idx & upper] = "  " + s[idx & upper].str.upper() + " "
    s[idx & ~upper] = s[idx & ~upper].str.lower() + "   "
    return s


def phone(rng, n):
    return pd.Series(rng.integers(1_000_000_000, 9_999_999_999, n)).map(
        lambda x: f"+7-{str(x)[:3]}-{str(x)[3:6]}-{str(x)[6:]}"
    )


def generate_notes(rng, n):
    """Build free-text notes with numbers and keywords hidden inside the text."""
    duration = pd.Series(rng.integers(20, 480, n)).astype(str)
    blood = pd.Series(rng.integers(50, 1500, n)).astype(str)
    allergy = pd.Series(rng.choice(ALLERGIES, n))
    compl = pd.Series(rng.choice(COMPLICATIONS, n))
    plain = pd.Series(rng.choice(PLAIN_NOTES, n))

    kind = pd.Series(rng.choice(5, n, p=[0.35, 0.2, 0.15, 0.1, 0.2]))
    notes = plain.astype("object").copy()

    m = kind == 1
    notes[m] = ("Duration " + duration + " min. Estimated blood loss: " + blood + " ml.")[m]
    m = kind == 2
    notes[m] = ("Allergic to " + allergy + " - alternative drugs administered. Duration: " + duration + " min.")[m]
    m = kind == 3
    notes[m] = ("Complication: " + compl + ". Blood loss " + blood + "ml, patient moved to ICU.")[m]
    m = kind == 4
    notes[m] = ("Op time " + duration + " MIN; " + plain)[m]

    # about 3% of notes are missing
    notes[rng.random(n) < 0.03] = None
    return notes


def write_csv_parts(df, folder, rows_per_file):
    os.makedirs(folder, exist_ok=True)
    for i, start in enumerate(range(0, len(df), rows_per_file)):
        df.iloc[start:start + rows_per_file].to_csv(
            os.path.join(folder, f"part-{i:04d}.csv"), index=False
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--operations", type=int, default=5_000_000)
    parser.add_argument("--doctors", type=int, default=2_000)
    parser.add_argument("--patients", type=int, default=1_000_000)
    parser.add_argument("--rows-per-file", type=int, default=1_000_000)
    parser.add_argument("--out", default="data/raw")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    rng = np.random.default_rng(args.seed)
    t0 = time.time()

    # Hospitals
    hospitals = pd.DataFrame({
        "hospital_id": range(1, len(HOSPITALS) + 1),
        "name": [h[0] for h in HOSPITALS],
        "city": [h[1] for h in HOSPITALS],
        "phone": phone(rng, len(HOSPITALS)),
    })

    # Doctors
    first, last = make_people(rng, args.doctors)
    doctors = pd.DataFrame({
        "doctor_id": range(1, args.doctors + 1),
        "hospital_id": rng.integers(1, len(HOSPITALS) + 1, args.doctors),
        "first_name": messy(rng, first),
        "last_name": messy(rng, last),
        "specialty": messy(rng, rng.choice(SPECIALTIES, args.doctors)),
        "phone": phone(rng, args.doctors),
    })

    # Patients
    first, last = make_people(rng, args.patients)
    birth = pd.Timestamp("1940-01-01") + pd.to_timedelta(rng.integers(0, 365 * 80, args.patients), unit="D")
    patients = pd.DataFrame({
        "patient_id": range(1, args.patients + 1),
        "hospital_id": rng.integers(1, len(HOSPITALS) + 1, args.patients),
        "first_name": messy(rng, first),
        "last_name": messy(rng, last),
        "date_of_birth": birth.strftime("%Y-%m-%d"),
        "gender": rng.choice(["M", "F"], args.patients),
    })

    # Operations: 2020-01-01 .. 2025-12-31, busier in some hospitals and on weekdays
    n = args.operations
    days = pd.Timestamp("2020-01-01") + pd.to_timedelta(rng.integers(0, 2192, n), unit="D")
    # move 60% of weekend operations to a random weekday of the same week
    dow = days.dayofweek.values
    move = (dow >= 5) & (rng.random(n) < 0.6)
    shift = np.where(move, dow - rng.integers(0, 5, n), 0)
    days = days - pd.to_timedelta(shift, unit="D")
    hospital_weights = rng.dirichlet(np.ones(len(HOSPITALS)) * 2)
    operations = pd.DataFrame({
        "operation_id": np.arange(1, n + 1),
        "patient_id": rng.integers(1, args.patients + 1, n),
        "doctor_id": rng.integers(1, args.doctors + 1, n),
        "hospital_id": rng.choice(np.arange(1, len(HOSPITALS) + 1), n, p=hospital_weights),
        "operation_name": messy(rng, rng.choice(OPERATIONS, n), share=0.03),
        "operation_date": days.strftime("%Y-%m-%d"),
        "notes": generate_notes(rng, n),
    })
    # about 0.5% duplicated rows, as happens when a source system sends data twice
    dupes = operations.sample(frac=0.005, random_state=args.seed)
    operations = pd.concat([operations, dupes]).sample(frac=1, random_state=args.seed)

    out = args.out
    write_csv_parts(hospitals, os.path.join(out, "hospitals"), args.rows_per_file)
    write_csv_parts(doctors, os.path.join(out, "doctors"), args.rows_per_file)
    write_csv_parts(patients, os.path.join(out, "patients"), args.rows_per_file)
    write_csv_parts(operations, os.path.join(out, "operations"), args.rows_per_file)

    print(f"Generated {len(hospitals)} hospitals, {len(doctors):,} doctors, "
          f"{len(patients):,} patients, {len(operations):,} operation rows "
          f"(incl. {len(dupes):,} duplicates) in {time.time() - t0:.1f}s -> {out}")


if __name__ == "__main__":
    main()
