import re
import json
import unicodedata
from typing import List, Optional, Any
from datetime import datetime, date
from pathlib import Path
from pydantic import BaseModel, Field, field_validator, ConfigDict, model_validator

DATA_FILE = Path(__file__).parent.parent / "data_job" / "itviec_jobs.json"
OUTPUT_FILE = Path(__file__).parent.parent / "data_job" / "itviec_jobs_schema.json"


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder for datetime and date objects"""
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super().default(obj)

# --- Location Mapping ---
LOCATION_MAPPING = {
    "ha_noi": ["Hà Nội", "Ha Noi", "ha noi", "Hanoi", "hanoi"],
    "tuyen_quang": ["Tuyên Quang", "Tuyen Quang", "tuyen quang", "Hà Giang", "Ha Giang", "ha giang"],
    "lao_cai": ["Lào Cai", "Lao Cai", "lao cai", "Yên Bái", "Yen Bai", "yen bai"],
    "thai_nguyen": ["Thái Nguyên", "Thai Nguyen", "thai nguyen", "Bắc Kạn", "Bac Kan", "bac kan"],
    "phu_tho": ["Phú Thọ", "Phu Tho", "phu tho", "Vĩnh Phúc", "Vinh Phuc", "vinh phuc", "Hòa Bình", "Hoa Binh", "hoa binh"],
    "bac_ninh": ["Bắc Ninh", "Bac Ninh", "bac ninh", "Bắc Giang", "Bac Giang", "bac giang"],
    "hung_yen": ["Hưng Yên", "Hung Yen", "hung yen", "Thái Bình", "Thai Binh", "thai binh"],
    "hai_phong": ["Hải Phòng", "Hai Phong", "hai phong", "Hải Dương", "Hai Duong", "hai duong"],
    "ninh_binh": ["Ninh Bình", "Ninh Binh", "ninh binh", "Nam Định", "Nam Dinh", "nam dinh", "Hà Nam", "Ha Nam", "ha nam"],
    "quang_tri": ["Quảng Trị", "Quang Tri", "quang tri", "Quảng Bình", "Quang Binh", "quang binh"],
    "da_nang": ["Đà Nẵng", "Da Nang", "da nang", "ĐàNẵng", "DaNang", "danang", "Quảng Nam", "Quang Nam", "quang nam"],
    "quang_ngai": ["Quảng Ngãi", "Quang Ngai", "quang ngai", "Kon Tum", "Kontum", "kon tum", "kontum"],
    "gia_lai": ["Gia Lai", "gia lai", "Bình Định", "Binh Dinh", "binh dinh"],
    "khanh_hoa": ["Khánh Hòa", "Khanh Hoa", "khanh hoa", "Ninh Thuận", "Ninh Thuan", "ninh thuan"],
    "lam_dong": ["Lâm Đồng", "Lam Dong", "lam dong", "Đắk Nông", "Dak Nong", "dak nong"],
    "dak_lak": ["Đắk Lắk", "Dak Lak", "dak lak", "Phú Yên", "Phu Yen", "phu yen"],
    "ho_chi_minh": ["TP. Hồ Chí Minh", "Ho Chi Minh", "ho chi minh", "TPHCM", "tp hcm", "tphcm", "Sài Gòn", "Sai Gon", "saigon", "Bình Dương", "Binh Duong", "binh duong", "Bà Rịa Vũng Tàu", "Ba Ria Vung Tau", "ba ria vung tau", "BRVT", "Vũng Tàu", "Vung Tau", "vung tau"],
    "dong_nai": ["Đồng Nai", "Dong Nai", "dong nai", "Bình Phước", "Binh Phuoc", "binh phuoc"],
    "tay_ninh": ["Tây Ninh", "Tay Ninh", "tay ninh", "Long An", "long an"],
    "can_tho": ["Cần Thơ", "Can Tho", "can tho", "Sóc Trăng", "Soc Trang", "soc trang", "Hậu Giang", "Hau Giang", "hau giang"],
    "vinh_long": ["Vĩnh Long", "Vinh Long", "vinh long", "Bến Tre", "Ben Tre", "ben tre", "Trà Vinh", "Tra Vinh", "tra vinh"],
    "dong_thap": ["Đồng Tháp", "Dong Thap", "dong thap", "Tiền Giang", "Tien Giang", "tien giang"],
    "ca_mau": ["Cà Mau", "Ca Mau", "ca mau", "Bạc Liêu", "Bac Lieu", "bac lieu"],
    "an_giang": ["An Giang", "an giang", "Kiên Giang", "Kien Giang", "kien giang"]
}

def denormalize_text(text: str) -> str:
    if not text: return ""
    text = unicodedata.normalize('NFD', text)
    text = ''.join([c for c in text if unicodedata.category(c) != 'Mn'])
    text = text.replace('đ', 'd').replace('Đ', 'd')
    return text.lower()

def map_to_standard_region(raw_location: str) -> Optional[str]:
    clean_input = denormalize_text(raw_location)
    for standard_name, aliases in LOCATION_MAPPING.items():
        for alias in aliases:
            if denormalize_text(alias) in clean_input:
                return standard_name
    return None

class JobSchema(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra='ignore')

    job_id: str
    source: str = "itviec"
    url: str = ""
    title: str = "No Title"
    
    company_name: str = "Unknown"
    company_url: Optional[str] = None
    company_id: Optional[str] = None
    company_size: Optional[str] = None
    company_industry: Optional[str] = None
    country: Optional[str] = None
    
    salary_raw: Optional[str] = Field(None, alias="salary")
    salary_min: Optional[int] = None
    salary_max: Optional[int] = None
    salary_currency: Optional[str] = None
    salary_negotiable: bool = False
    
    location_raw: Optional[str] = Field(None, alias="location")
    locations: List[str] = Field(default_factory=list)
    work_mode: str = "onsite"
    
    job_level: Optional[str] = Field(None, alias="job_expertise")
    experience_years_min: int = 0
    education: Optional[str] = None
    work_mode_days: Optional[str] = Field(None, alias="working_days")
    overtime_policy: Optional[str] = Field(None, alias="overtime_policy")
    hiring_quantity: int = 1
    
    deadline: Optional[date] = None
    posted_date: Optional[datetime] = Field(None, alias="posted_datetime")
    crawled_date: Optional[datetime] = None
    
    skills: List[str] = Field(default_factory=list)
    job_domains: List[str] = Field(default_factory=list)
    description: str = ""
    requirements: Optional[str] = None
    benefits: Optional[str] = None

    @field_validator("company_size", mode="before")
    @classmethod
    def normalize_company_size(cls, v: Any) -> Optional[str]:
        if not v:
            return None

        s = str(v).lower().strip()

        # Strip unit words
        s = re.sub(r'\b(nhan\s*vien|nhân\s*viên|nguoi|người|employees?|staffs?)\b', '', s, flags=re.IGNORECASE)
        s = re.sub(r'\s{2,}', ' ', s).strip(' ,;:-')

        if not s:
            return None

        # Remove commas in numbers: 1,000 → 1000
        s = re.sub(r'(\d),(\d)', r'\1\2', s)

        # Normalize "hơn 1000" / "over 1000" / "more than 1000" → ">1000"
        s = re.sub(r'\b(hon|hơn|over|tren|trên|more\s*than|greater\s*than)\s*(\d+)', r'>\2', s, flags=re.IGNORECASE)

        # Normalize "dưới 50" / "under 50" / "less than 50" → "<50"
        s = re.sub(r'\b(duoi|dưới|under|less\s*than|fewer\s*than)\s*(\d+)', r'<\2', s, flags=re.IGNORECASE)

        # Normalize "1000+" → ">1000"
        s = re.sub(r'(\d+)\s*\+', r'>\1', s)

        # Normalize range separators → "min-max"
        s = re.sub(r'\s*[-–—~]\s*', '-', s)
        s = re.sub(r'\s+(to|toi|tới|đến|den)\s+', '-', s, flags=re.IGNORECASE)

        return s.strip() or None

    @field_validator("work_mode", mode="before")
    @classmethod
    def normalize_work_mode(cls, v: Any) -> str:
        if not v:
            return "onsite"
        
        s = str(v).lower().strip()
        s = (s
            .replace("à","a").replace("á","a").replace("ả","a").replace("ã","a").replace("ạ","a")
            .replace("ă","a").replace("ắ","a").replace("ặ","a").replace("ằ","a").replace("ẳ","a").replace("ẵ","a")
            .replace("â","a").replace("ấ","a").replace("ầ","a").replace("ẩ","a").replace("ẫ","a").replace("ậ","a")
            .replace("è","e").replace("é","e").replace("ẻ","e").replace("ẽ","e").replace("ẹ","e")
            .replace("ê","e").replace("ế","e").replace("ề","e").replace("ể","e").replace("ễ","e").replace("ệ","e")
            .replace("ì","i").replace("í","i").replace("ỉ","i").replace("ĩ","i").replace("ị","i")
            .replace("ò","o").replace("ó","o").replace("ỏ","o").replace("õ","o").replace("ọ","o")
            .replace("ô","o").replace("ố","o").replace("ồ","o").replace("ổ","o").replace("ỗ","o").replace("ộ","o")
            .replace("ơ","o").replace("ớ","o").replace("ờ","o").replace("ở","o").replace("ỡ","o").replace("ợ","o")
            .replace("ù","u").replace("ú","u").replace("ủ","u").replace("ũ","u").replace("ụ","u")
            .replace("ư","u").replace("ứ","u").replace("ừ","u").replace("ử","u").replace("ữ","u").replace("ự","u")
            .replace("ỳ","y").replace("ý","y").replace("ỷ","y").replace("ỹ","y").replace("ỵ","y")
            .replace("đ","d")
        )

        HYBRID_PATTERNS = [
            r'\bhybrid\b',
            r'\blinh\s*hoat\b',
            r'\bban\s*thoi\s*gian\b',
            r'\bpart\s*time\b',
            r'\bpart-time\b',
            r'\bflexible\b',
            r'\bket\s*hop\b',
            r'\blam\s*viec\s*linh\s*hoat\b',
            r'\bco\s*the\s*remote\b',
            r'\bmot\s*phan\b',
            r'\bnua\s*thoi\s*gian\b',
            r'\bpartly\s*remote\b',
            r'\bpartial\s*remote\b',
            r'\bpartially\s*remote\b',
            r'\bmot\s*phan\s*remote\b',
            r'\bremote\s*mot\s*phan\b',
        ]

        REMOTE_PATTERNS = [
            r'\bremote\b',
            r'\bwfh\b',
            r'\bwork\s*from\s*home\b',
            r'\bwork-from-home\b',
            r'\btu\s*xa\b',
            r'\bo\s*nha\b',
            r'\blam\s*o\s*nha\b',
            r'\blam\s*viec\s*o\s*nha\b',
            r'\bfull\s*remote\b',
            r'\b100%\s*remote\b',
            r'\bfully\s*remote\b',
            r'\bdistributed\b',
            r'\btelework\b',
            r'\btelecommut\w+\b',
        ]

        ONSITE_PATTERNS = [
            r'\bonsite\b',
            r'\bon-site\b',
            r'\bon\s*site\b',
            r'\bin\s*office\b',
            r'\bin-office\b',
            r'\btai\s*van\s*phong\b',
            r'\bvan\s*phong\b',
            r'\bfull\s*time\b',
            r'\bfull-time\b',
            r'\btoan\s*thoi\s*gian\b',
            r'\bpresential\b',
            r'\bin\s*person\b',
        ]

        for pattern in HYBRID_PATTERNS:
            if re.search(pattern, s):
                return "hybrid"

        for pattern in REMOTE_PATTERNS:
            if re.search(pattern, s):
                return "remote"

        for pattern in ONSITE_PATTERNS:
            if re.search(pattern, s):
                return "onsite"

        return "onsite"

    @field_validator("locations", mode="before")
    @classmethod
    def handle_location_mapping(cls, v: Any, info: Any) -> List[str]:
        # Read directly from 'location' (key from raw input)
        raw = str(v) if v else str(info.data.get("location", ""))
        # Split by multiple separators: comma, pipe, semicolon, dash, ampersand
        parts = re.split(r'[,|;\-&]', raw)
        mapped = [map_to_standard_region(p.strip()) for p in parts if p.strip()]
        # Remove duplicates while preserving order
        seen = set()
        result = []
        for m in mapped:
            if m and m not in seen:
                result.append(m)
                seen.add(m)
        return result
    
    @model_validator(mode="before")
    def map_itviec_fields(self) -> dict:
        # Extract job_level and job_domains from job_expertise
        if "job_expertise" in self and isinstance(self["job_expertise"], list) and self["job_expertise"]:
            if "job_level" not in self or not self["job_level"]:
                self["job_level"] = str(self["job_expertise"][0]).lower()
            # Also populate job_domains with all job_expertise items
            if "job_domains" not in self or not self["job_domains"]:
                self["job_domains"] = [str(exp).lower() for exp in self["job_expertise"]]
        
        # Append top_reasons to benefits
        if "top_reasons" in self and isinstance(self["top_reasons"], list) and self["top_reasons"]:
            reasons_str = ";".join(self["top_reasons"])
            if self.get("benefits"):
                self["benefits"] = str(self["benefits"]) + f"\n\n[Others: {reasons_str}]"
            else:
                self["benefits"] = f"[Others: {reasons_str}]"
        
        return self
    
    @model_validator(mode="before")
    def parse_salary_fields(self) -> dict:
        # Parse salary_raw to extract min and max values and currency
        salary_raw = str(self.get("salary") or self.get("salary_raw") or "")
        if salary_raw and "thoa thuan" not in salary_raw.lower() and "negotiable" not in salary_raw.lower():
            try:
                # Extract numbers with optional commas
                numbers = re.findall(r'[\d,]+', salary_raw)
                if numbers:
                    # Clean numbers (remove commas and convert to int)
                    cleaned_nums = [int(num.replace(',', '')) for num in numbers]
                    if cleaned_nums:
                        self["salary_min"] = min(cleaned_nums)
                        self["salary_max"] = max(cleaned_nums)
                
                # Extract currency unit
                # Match USD, VND, EUR, etc. or Vietnamese units like triệu, nghìn
                currency_match = re.search(r'(USD|VND|EUR|GBP|JPY|triệu|ngh\w+n|\$|€|¥|₹)', salary_raw, re.IGNORECASE)
                if currency_match:
                    currency = currency_match.group(1).lower()
                    # Normalize Vietnamese currency
                    if currency == "triệu" or currency == "triệu":
                        self["salary_currency"] = "VND (million)"
                    elif "ngh" in currency:
                        self["salary_currency"] = "VND (thousand)"
                    elif currency == "$":
                        self["salary_currency"] = "USD"
                    elif currency == "€":
                        self["salary_currency"] = "EUR"
                    elif currency == "¥":
                        self["salary_currency"] = "JPY"
                    else:
                        self["salary_currency"] = currency.upper()
            except:
                pass
        return self
    
    @model_validator(mode="before")
    def set_company_industry_from_type(self) -> dict:
        # If company_industry is not provided, try to get it from company_type
        if not self.get("company_industry") and self.get("company_type"):
            self["company_industry"] = self["company_type"]
        return self

    @model_validator(mode="before")
    def sync_posted_fields(self) -> dict:
        # Keep compatibility with older keys and crawler versions
        if not self.get("posted_date") and self.get("posted_at"):
            self["posted_date"] = self["posted_at"]
        if not self.get("posted_date") and self.get("posted_datetime"):
            self["posted_date"] = self["posted_datetime"]
        return self
    
    @model_validator(mode="after")
    def populate_locations_and_negotiable(self) -> "JobSchema":
        # Populate locations from location_raw if empty
        if not self.locations and self.location_raw:
            # Split by multiple separators: comma, pipe, semicolon, dash, ampersand
            parts = re.split(r'[,|;\-&()]', self.location_raw)
            mapped = [map_to_standard_region(p.strip()) for p in parts if p.strip()]
            # Remove duplicates while preserving order
            seen = set()
            result = []
            for m in mapped:
                if m and m not in seen:
                    result.append(m)
                    seen.add(m)
            self.locations = result
        
        # Check if salary is negotiable
        if self.salary_raw:
            raw = str(self.salary_raw).lower()
            if re.search(r"(thoa.*thuan|negotiable|canh.*tranh|thuong.*luong|t\.t|tt|upto|you.*ll love|love it)", raw):
                self.salary_negotiable = True
        
        return self

    @field_validator("job_level", mode="before")
    @classmethod
    def normalize_job_level(cls, v: Any, info: Any) -> Optional[str]:
        # Read from 'job_expertise' (key from raw input) or nested fields
        val = v
        if not val:
            val = info.data.get("job_expertise")
        if isinstance(val, list) and val:
            return str(val[0]).lower()
        return str(val).lower() if val else None

    @field_validator("company_industry", mode="before")
    @classmethod
    def map_industry(cls, v: Any, info: Any) -> Optional[str]:
        # Read from possible source keys
        return v or info.data.get("company_industry") or info.data.get("company_type")

if __name__ == "__main__":
    # Load data from JSON file
    data_file = DATA_FILE
    output_file = OUTPUT_FILE
    
    if not data_file.exists():
        print(f"Error: File not found: {data_file}")
        exit(1)
    
    with open(data_file, "r", encoding="utf-8") as f:
        sample_jobs = json.load(f)
    
    print(f"Loaded {len(sample_jobs)} jobs from {data_file.name}\n")
    
    processed_jobs = []
    
    # Process all jobs
    for i, data in enumerate(sample_jobs):
        try:
            job = JobSchema(**data)
            processed_jobs.append(job.model_dump())
            
            # Print progress for first 3 jobs
            if i < 3:
                print(f"[{i+1}] ID: {job.job_id} | Title: {job.title}")
                print(f"    Location: {job.locations} | Negotiable: {job.salary_negotiable}")
                print(f"    Industry: {job.company_industry} | Level: {job.job_level}\n")
        except Exception as e:
            print(f"[{i+1}] Error processing job {data.get('job_id', 'N/A')}: {e}")
    
    # Save processed data to file
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(processed_jobs, f, indent=2, ensure_ascii=False, cls=DateTimeEncoder)
    
    print(f"\n✓ Processed {len(processed_jobs)} jobs")
    print(f"✓ Output saved to: {output_file.name}")
