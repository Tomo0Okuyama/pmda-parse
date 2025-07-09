# PMDA Medical Drug Information Parser

A Python parser that extracts structured medical information from PMDA (Pharmaceuticals and Medical Devices Agency) XML/SGML pharmaceutical data files and generates JSON data optimized for vector search.

## Overview

### About PMDA

**PMDA (Pharmaceuticals and Medical Devices Agency)** is an independent administrative agency of Japan that conducts consistent regulatory operations from clinical consultation to approval review and post-marketing safety measures for the quality, efficacy, and safety of pharmaceuticals and medical devices. PMDA publicly provides package insert information for all approved pharmaceuticals in XML/SGML format, which contains the following important medical information:

- **Approval Information**: Indications, dosage and administration
- **Safety Information**: Adverse reactions, contraindications, warnings and precautions
- **Drug Interaction Information**: Precautions for concomitant use with other drugs or foods
- **Ingredient Information**: Details of active ingredients and additives
- **Manufacturer Information**: Pharmaceutical companies, approval numbers, etc.

### Repository Purpose

This repository supports the construction of systems that allow physicians to efficiently search for appropriate pharmaceuticals from diagnosis and medical practice by extracting and structuring the above information from PMDA's publicly available pharmaceutical database.

### Extracted Information

**Essential Information:**
- Product ID
- Product Name
- YJ Code
- Dosage Form (Formulation:ColorTone format or appearance information)
- Manufacturer (company code and company name)
- Source filename

**Medical Information (structured by conditions and severity):**
- **Indications**: Therapeutic effects for target diseases and symptoms
- **Dosage and Administration**: Condition-specific administration methods (e.g., `Hypertension:Adult:Usually...`)
  - **Complex Administration Protocol Support**: A-method to F-method patterns for anticancer drugs with body surface area dosage tables
  - **Supported Drugs**: Capecitabine, TS-1, Irinotecan, Paclitaxel, etc.
  - **Medical Procedure Dosage Tables**: Structured extraction for contrast agents (e.g., `Cerebral angiography → Iopamidol 300 injection "F":6-13mL`)
- **Contraindications**: Patient backgrounds, concomitant drugs, and diseases where administration is prohibited
- **Warnings and Precautions**: Important precautions and risks during administration
- **Adverse Reactions**: Severity-classified adverse events (e.g., `Serious:Angioedema:Description`, `Non-serious:Hypertension:Rash`)
- **Drug Interactions**: Interactions with concomitant drugs or foods
- **Composition**: Name and content information of active ingredients and additives (structured)
- **Active Ingredient Details**: Physicochemical information (generic name, chemical name, molecular formula, molecular weight, properties, etc.)

## Features

### 1. Condition-Specific Dosage Extraction
Properly processes nested Header structures in XML to extract dosage according to diseases and patient conditions:
```json
"dosage": [
  "Hypertension:Adult:Usually, 4-8mg administered orally once daily for adults...",
  "Hypertension:Pediatric:Usually, 0.05-0.3mg/kg administered orally once daily for children aged 1-6 years...",
  "Renal parenchymal hypertension:Usually, start with 2mg orally once daily for adults..."
]
```

### 1-2. Complex Anticancer Administration Protocol Support
Automatic detection and analysis of complex administration methods (A-method to F-method) and body surface area dosage tables used for anticancer drugs:
```json
"dosage": [
  "Select the following administration method according to the indication",
  "A-method:Twice daily after breakfast and dinner, 2-3 tablets each time for 21 consecutive days, then 7 days rest/Body surface area <1.25m²:2 tablets morning, 1 tablet evening, 1.25-1.5m²:2 tablets morning, 2 tablets evening, ≥1.5m²:3 tablets morning, 2 tablets evening",
  "B-method:Twice daily after breakfast and dinner, 2-3 tablets each time for 14 consecutive days, then 7 days rest/Body surface area <1.25m²:2 tablets morning, 1 tablet evening, 1.25-1.5m²:2 tablets morning, 2 tablets evening, ≥1.5m²:3 tablets morning, 2 tablets evening"
]
```

### 1-3. Medical Procedure Dosage Table Support
Structured extraction of medical procedure-specific dosage tables for contrast agents and others:
```json
"dosage": [
  "Usually, the following amount is used once in adults. Adjust appropriately according to age, weight, symptoms, and purpose.",
  "Cerebral angiography → Iopamidol 300 injection \"F\":6-13mL",
  "Cardiac angiography (including pulmonary angiography) → Iopamidol 370 injection \"F\":20-50mL",
  "Aortic angiography → Iopamidol 300 injection \"F\":30-50mL / Iopamidol 370 injection \"F\":30-50mL",
  "Computed tomography contrast imaging → Iopamidol 150 injection \"F\":200mL / Iopamidol 300 injection \"F\":100mL / Iopamidol 370 injection \"F\":100mL",
  "Note: Use undiluted or diluted 2-4 times with physiological saline.",
  "Note: When administering 50mL or more, usually use intravenous drip."
]
```

### 2. Severity-Classified Adverse Reaction Information
Clear classification of serious adverse reactions and other adverse reactions (severity-priority format):
```json
"side_effects": [
  "Serious:Angioedema (frequency unknown):Angioedema characterized by swelling of face, lips, tongue, pharynx, larynx, etc. may occur...",
  "Non-serious:Hypertension:Dizziness, unsteadiness, lightheadedness, palpitations...",
  "Non-serious:Chronic heart failure:Rash, eczema, urticaria..."
]
```

### 3. High-Performance Duplicate Removal
- File duplicate detection using SHA-256 hash
- Complete duplicate removal within categories
- Efficient processing of large datasets

### 4. Precise Composition Extraction
Accurate pairing of active ingredients and additives with their names and weights (structured output):
```json
"compositions": [
  "Active ingredient:Alteplase (recombinant) 12 million international units",
  "Additive:L-Arginine 653mg",
  "Additive:Polysorbate 80 1.8mg",
  "Additive:Phosphoric acid"
]
```

### 5. Complete Data Retention
- No data reduction filters
- Preserves all valid medical information
- Supports detailed search needs of healthcare professionals
- Preserves original values ("250 international units" kept intact)

### 6. Active Ingredient Detailed Information
Extraction of physicochemical information from PhyschemOfActIngredients section (excluding structural formula information):
```json
"active_ingredients": [
  {
    "general_name": "Levocetirizine Hydrochloride",
    "chemical_name": "2-(2-[4-[(R)-(4-Chlorophenyl)phenylmethyl]piperazin-1-yl]ethoxy)acetic acid dihydrochloride",
    "molecular_formula": "C21H25ClN2O3・2HCl",
    "molecular_weight": "461.81",
    "nature": "White crystalline powder."
  }
]
```

### 7. Medical Procedure Dosage Table Support
- Automatic detection and analysis of SimpleTable structures for contrast agents
- Structured extraction of medical procedure names with corresponding drugs and dosages
- Support for multiple drug concentrations simultaneously (e.g., Iopamidol 150/300/370)
- Proper extraction of table-related precautions

### 8. XML Structure-Based Analysis
- Information extraction based on XML structure rather than text content
- Robust analysis unaffected by variations in documentation between manufacturers
- Full compliance with PMDA standard XML namespaces

### 9. Enhanced Dosage Form Extraction
- Combination of Formulation + ColorTone elements (e.g., "Plain tablet:White")
- Fallback to appearance/property information from PropertyForConstituentUnits
- Alternative acquisition from DosageForm and therapeutic classifications

### 10. Original Value Preservation
- Preserves XML/SGML values without splitting (e.g., "250 international units")
- Expresses meaning through JSON structure

## Repository Structure

```
pmda-parse/
├── .gitignore                     # Git exclusion file settings
├── README.md                      # This file (Japanese)
├── README_EN.md                   # This file (English)
├── src/
│   ├── pmda_json_generator.py      # Main execution file
│   ├── parsers/                    # Medical information parser modules
│   │   ├── base_parser.py          # Base parser (essential information extraction)
│   │   ├── indication_parser.py    # Indications parser
│   │   ├── dosage_parser.py        # Dosage parser (complex protocol support)
│   │   ├── contraindication_parser.py  # Contraindications parser
│   │   ├── warning_parser.py       # Warnings and precautions parser
│   │   ├── side_effect_parser.py   # Adverse reactions parser (severity support)
│   │   ├── interaction_parser.py   # Drug interactions parser
│   │   ├── composition_parser.py   # Composition parser (structured)
│   │   ├── active_ingredient_parser.py  # Active ingredient details parser
│   │   └── xml_utils.py           # Common XML utility functions
│   └── utils/                      # Utility modules
│       └── file_processor.py       # File processing and duplicate removal
├── pmda_all_20250629/             # PMDA data directory (excluded by .gitignore)
└── pmda_medicines.json           # Output JSON file (excluded by .gitignore)
```

## Usage

### Environment Setup

```bash
# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# or
.venv\Scripts\activate     # Windows

# Install dependencies
pip install -r requirements.txt
```

### Basic Execution

```bash
# Process all pharmaceutical data (default output: pmda_medicines.json)
python src/pmda_json_generator.py

# Specify output file
python src/pmda_json_generator.py --output /path/to/output.json

# Process specific directory data
python src/pmda_json_generator.py --data-dir /path/to/pmda_data
```

### Execution Example

```bash
$ python src/pmda_json_generator.py
File scanning started...
Detecting and removing duplicate files...
Processing pharmaceutical data...

=== Processing Complete Summary ===
Total pharmaceuticals: 11,464
Duplicate-removed files: 8,234
Output file: pmda_medicines.json (64.35 MB)

Medical information statistics:
- Indications: 45,123 items
- Dosage and administration: 42,891 items
- Adverse reactions: 89,456 items (Serious: 12,345, Non-serious: 77,111)
- Contraindications: 23,567 items
- Warnings and precautions: 34,789 items
- Drug interactions: 18,234 items
- Composition information: 56,789 items
```

## Output Data Format

```json
{
  "product_id": "670109_7219412A8082_1_04",
  "product_name": "Candesartan Tablets 8mg \"DSEP\"",
  "yj_code": "2149040F3061",
  "form": "Sustained-release angiotensin II receptor antagonist",
  "manufacturer_code": "430773",
  "manufacturer_name": "Daiichi Sankyo Espha Co., Ltd.", 
  "source_filename": "430773_2149040F1069_1_10.xml",
  "clinical_info": {
    "indications": [
      "Hypertension",
      "Renal parenchymal hypertension",
      "Chronic heart failure"
    ],
    "dosage": [
      "Hypertension:Adult:Usually, 4-8mg administered orally once daily for adults, increase to 12mg as needed",
      "Hypertension:Pediatric:Usually, 0.05-0.3mg/kg administered orally once daily for children aged 1-6 years",
      "Renal parenchymal hypertension:Usually, start with 2mg orally once daily for adults, increase to 8mg as needed"
    ],
    "contraindications": [
      "Patients with a history of hypersensitivity to the components of this drug",
      "Pregnant women or women who may be pregnant"
    ],
    "warnings": [
      "Angioedema may occur, so observe carefully"
    ],
    "side_effects": [
      "Serious:Angioedema (frequency unknown):Angioedema characterized by swelling of face, lips, tongue, pharynx, larynx, etc. may occur",
      "Non-serious:Hypertension:Dizziness, unsteadiness, lightheadedness, palpitations, hot flashes",
      "Non-serious:Chronic heart failure:Rash, eczema, urticaria, pruritus"
    ],
    "interactions": [
      "Drug:Lithium preparations - Mechanism:Reabsorption of lithium in renal tubules is promoted"
    ],
    "compositions": [
      "Active ingredient:Candesartan cilexetil 8.0mg",
      "Additive:L-Histidine 7.8mg",
      "Additive:Polysorbate 80 0.5mg",
      "Additive:Phosphoric acid"
    ],
    "active_ingredients": [
      {
        "general_name": "Candesartan cilexetil",
        "chemical_name": "2-Ethoxy-1-{[2'-(1H-tetrazol-5-yl)biphenyl-4-yl]methyl}-1H-benzimidazole-7-carboxylic acid",
        "molecular_formula": "C24H20N6O3",
        "molecular_weight": "440.45",
        "nature": "White crystalline powder."
      }
    ]
  }
}
```

## Technical Features

### Advanced XML Parser Functions

1. **Nested Header Structure Processing**
   - Proper analysis of hierarchical structures of disease conditions and subjects (adult/pediatric)
   - Support for complex XML structures through recursive Item element processing

2. **Namespace Support**
   - Proper handling of PMDA XML namespaces
   - `http://info.pmda.go.jp/namespace/prescription_drugs/package_insert/1.0`

3. **Automatic Severity Classification**
   - SeriousAdverseEvents → Serious
   - OtherAdverseEvents → Non-serious
   - Severity-priority format (`Non-serious:Condition:Adverse reaction`)
   - Support for clinical decision-making by healthcare professionals

4. **Precise Composition Parsing**
   - Paired extraction of active ingredient names and contents from `ContainedAmount` elements
   - Paired extraction of additive names and contents from `InfoIndividualAdditive` elements
   - Output ingredient names only when weight information is unavailable
   - Category-based text output ("Active ingredient:Name Content", "Additive:Name Content" format)

5. **Active Ingredient Detailed Information Extraction**
   - Extraction of physicochemical information from PhyschemOfActIngredients section
   - Detailed information including generic name, chemical name, molecular formula, molecular weight, properties, etc.
   - Proper handling of XML tags (Italic, Sub, Sup, etc.)
   - **Structural formula information excluded** (unnecessary for physician prescribing decisions)

6. **Complex Administration Protocol Support**
   - Automatic detection of A-method to F-method patterns in anticancer drugs
   - Analysis and integration of body surface area dosage tables
   - Supported drugs: Capecitabine, TS-1, Irinotecan, Paclitaxel, etc.
   - Extraction of body surface area ranges and corresponding dosages from TblBlock elements

7. **Medical Procedure Dosage Table Support**
   - Automatic detection and analysis of SimpleTable structures for contrast agents
   - Structured extraction of medical procedure names with corresponding drugs and dosages
   - Support for multiple drug concentrations simultaneously (e.g., Iopamidol 150/300/370)
   - Proper extraction of table-related precautions

8. **XML Structure-Based Analysis**
   - Analysis based on XML structure rather than text content
   - Robust information extraction unaffected by variations between companies
   - Full compliance with PMDA standard XML namespaces

9. **Original Value Preservation**
   - Preserves XML/SGML values without splitting (e.g., "250 international units")
   - Expresses meaning through JSON structure

### Performance Optimization

- **File Duplicate Removal**: High-speed duplicate detection using SHA-256 hash
- **Memory Efficiency**: Optimized for large dataset processing
- **Parallel Processing Support**: Design prepared for future expansion

## Development & Customization

### Adding New Parsers

1. Create new parser file in `src/parsers/`
2. Utilize common XML utility functions (`xml_utils.py`)
3. Call parser in `pmda_json_generator.py`
4. Update JSON schema when supporting new medical information categories

### Running Tests

```bash
# Testing with specific pharmaceuticals
python test_nested_dosage.py
python test_side_effects.py

# Testing condition-specific dosage
python test_condition_dosage_final.py
```

## License

This repository is released under the MIT License.

## Contributing

We welcome pull requests and issue reports. We prioritize the accuracy and safety of medical information in our development.

### Development Policy
- **Safety First**: Highest priority on accuracy and reliability of medical data
- **Extensibility**: Support for new pharmaceutical categories and information types
- **Type Safety**: Robustness through TypeScript-style Python type annotations
- **Test-Driven**: Operational verification of important medical information parsers

## Disclaimer

- This tool is intended to support healthcare professionals in information retrieval and is not a substitute for medical judgment
- For medical use of extracted data, always verify with the latest package inserts
- Use of PMDA data should comply with related terms of use