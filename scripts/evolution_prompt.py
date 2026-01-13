import requests
import time
import sys

# הגדרת קידוד למניעת שגיאות בטרמינל
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')

# רשימת המפתחות שלך
API_KEYS = [
    "AIzaSyBhMSKDM_N0IyPL5aqnboLNMlW6ZLIHRBs", "AIzaSyAoUUw0ZRB_d8qNh5I-LAROfVcQH3qdM8I",
    "AIzaSyAiuHrHqNAHFqbnMWAmH0j74f6c1o5NZ7Y", "AIzaSyD6BH7Xz9XlbCW_SlpAuPemeJ2nbHRAILY",
    "AIzaSyCDJV1OrPKi4DfJl8zIszhKpB_9tC-CF_M", "AIzaSyCKeRGgt4jiLLrtWNFhtCVjf1Z0gm8iS4I",
    "AIzaSyBQrpk3LqY56RByWzIJKJPIA5NDyr6d_rQ", "AIzaSyBm6ZhL1lGGWufhv9j4gFZQ_9EE0lqR_uc",
    "AIzaSyD5dehVjUWo3sWTr9N11qflHxOJ_1CGK1c", "AIzaSyBm6ZhL1lGGWufhv9j4gFZQ_9EE0lqR_uc",
    "AIzaSyALHbWLBHGJWRUafa4fub_qBbZEc8CLwxM",
    
    "AIzaSyAG3aSb0MoA26yXYaswnuWkjbf8zKj9nJU", "AIzaSyAbaRtVOAmujN4Eh0k2UP1sY7QHehmcHFc",
    "AIzaSyCfsNgf7WArIH71Y_B5N_vlAo_5Y7mSv2s", "AIzaSyBPc_VKzCiFtoQZJOj1EDEh0KQfs8t9VNM",
    "AIzaSyD_Y75GhTop19Kjv54JM_vWDuAOxDsYWbg", "AIzaSyC8PbxwE9djNz_CKn99NngFcR5ue9qrDAQ",
    "AIzaSyBvDOGiMCNdrUkDmtsJExBuX_C3zih1gZY", "AIzaSyAwlLZi1bRaT7_rykX8u1baONkpEGomNEY",
    "AIzaSyAvqagx625EF1HkLKKHS-7NWI61h5a0clE", "AIzaSyCdnCmFN3RhgfiwfKBibkhHn2PUaG3W65A",
    
  "AIzaSyCAO0p4dCRqhkXKS51_SuBumqIMIBslkqw", "AIzaSyB5ehL0AzEZYuP-X6LZQtc952_Z0p8wiLc",
    "AIzaSyDLaV9a1cRMnKEr5Oty8v_K9HmdVTug4Hc", "AIzaSyAOAFdkAo6_R5hPuc0jyBrM225pn64XB-w",
    "AIzaSyBGdreRMb9zt79xUICmPqomvAMdWFn3RFU", "AIzaSyAbkxAgnPruaTUm6G3D2kTdSZxPIcHBZIQ",
    "AIzaSyAlQhGowFFu0yTSn4WSv-aC-upKy4Kcw4k", "AIzaSyCvlpyvoEfrjknsXIcFUTWtLNb2iEz17RY", 
    "AIzaSyA514WRfN0fN8XYuD23DFy28s95PUOorv4", "AIzaSyDAa4145fz-Wzla9UNzwrC_f3_UvB6XgLM",
    "AIzaSyDJICglPJmMKwWZ-UlGCIppzb4MwWEUMn4", "AIzaSyDUCDz3Ew96jRlL8Ro50UyNPHEE6Yp5AP4",
    "AIzaSyCWhzLM02gsA9WDgMwOQhCIpp-AdiKuX-c", "AIzaSyCsR1RKULQIgYzhcTD52JZT8q5j69EmZE4",
    "AIzaSyCsR1RKULQIgYzhcTD52JZT8q5j69EmZE4", "AIzaSyAAMHMxFDOEF3MfRUf3btSWjYcMQVwh1GA",
    "AIzaSyBidP59grFFC8MDhwN_s7RpIUQCiTl5K-Y", "AIzaSyCteppIfVYO9R1l1ziPU6p9FbcoBaszHe0",
    
    "AIzaSyAwxQdd5w3XNsuCR5cCl9PvRkwTCE2JgIg","AIzaSyBi9Qf_9GLAO4azyhZRxGcnj4fLgmush_Y","AIzaSyDSsz6tVHP6uG8Ka2wHcku7KQ7X9hHFuEM","AIzaSyAdp5E-ZOcUIqgxG3wA6IbFpnvY7VC1eN0","AIzaSyDBI5cbhOxJF0iYLSAYGfFzxJEp2EJvBXQ",
    "AIzaSyDtm_v-OGUJAmws0YYjK0O3S-qdxgKSqCQ","AIzaSyBoyvtv6fmu6QAln4jaaRkAmirPh5tyHvI","AIzaSyDNrJVjTdANA36XLggKbD5POxaKphDOfaE","AIzaSyA0dhvuCCsLK4uxvnh7KpdhP_g_sDYpCAI","AIzaSyDsJvarSqllcFbjl-3vWgFidI4IpPOvAMU",
    "AIzaSyBxNbVLUcGvBsbISYqq4GRyZFAZpUMlBGA", "AIzaSyCOUO9JNveZ3mXSuMWVWZys3F_iV8x8zos", 
    "AIzaSyD5i5QQKFsQERCIIyo2nxbVxpjAfuK76d0", "AIzaSyAxajijFg9GOuhs6AIyqSfobNnAMPBs_ek", 
    "AIzaSyDHKWdtQM5zpBB2Bt6Vn1CxzxKUjZOM_Jc","AIzaSyCAwqDCQ1wy2Q7SJPydXN2HvvoMOua0-4Y", 
    "AIzaSyB-niqNmQqFSFmtbJ1Zo-vG1UAd18gGlmo", "AIzaSyBacSY94S5IJ7JUblP5_TxGF0A1yUD6FUM", 
    "AAIzaSyA-9KhINucQJJxC_5IR0CIEHKXF92pfxHY", "AIzaSyDlCzyK7JtdjZ4E-l6FkcDLsEgaWx-ms6U",
    "AIzaSyAsNmPt-us1qeKc00jX1xDx8kyOr9hI5-w","AIzaSyDDlx8K7Fn6AwICo0Bc5jr3SCVrubhoqww",
    
]


current_key_index = 0

class Chunk:
    def __init__(self, chunk_id, text):
        self.chunk_id = chunk_id
        self.text = text

def run_evolution_llm(query: str, early_chunks: list, late_chunks: list) -> str:
    """
    מבצע את הניתוח האבולוציוני תוך שימוש בניהול המפתחות והגרסה שעבדו בשלב הקודם.
    """
    global current_key_index
    
    # 1. עיבוד הטקסטים
    early_text = "\n".join([f"[ID: {c.chunk_id}] {c.text}" for c in early_chunks])
    late_text = "\n".join([f"[ID: {c.chunk_id}] {c.text}" for c in late_chunks])

    # 2. פרומפט הניתוח האבולוציוני
    system_prompt = (
        "You are an expert temporal analysis assistant. Your goal is to identify and explain "
        "how views, policies, or situations have CHANGED over time.\n\n"
        "Your response must follow this structure:\n"
        "1. EARLY POSITION: Describe the stance or status during the early period.\n"
        "2. LATE POSITION: Describe the stance or status during the late period.\n"
        "3. THE EVOLUTION: Explicitly explain what changed between these periods and why, if mentioned.\n\n"
        "RULES:\n"
        "- Use ONLY the provided documents.\n"
        "- Mention if there is a clear trend or if positions remained stable."
    )

    user_message = (
        f"Question: {query}\n\n"
        f"--- EARLY PERIOD DOCUMENTS ---\n{early_text}\n\n"
        f"--- LATE PERIOD DOCUMENTS ---\n{late_text}"
    )

    #  לוגיקת הניסיונות והמפתחות
    retries = 27*2
    for i in range(retries):
        if current_key_index >= len(API_KEYS):
            return "Error: All API keys exhausted."

        current_key = API_KEYS[current_key_index]
        
        # URL (Gemini 2.5 Flash Preview)
        url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-09-2025:generateContent?key={current_key}"

        payload = {
            "contents": [{"parts": [{"text": user_message}]}],
            "systemInstruction": {"parts": [{"text": system_prompt}]},
            "generationConfig": {"temperature": 0}
        }

        try:
            response = requests.post(url, json=payload)

            if response.status_code == 200:
                result = response.json()
                return result.get('candidates', [{}])[0].get('content', {}).get('parts', [{}])[0].get('text', "No answer generated.")
            
            # ניהול מכסות
            if response.status_code in [429, 403, 404]:
                print(f"Key {current_key_index + 1} issue (Status {response.status_code}). Switching key...")
                current_key_index += 1
                time.sleep(10)
                continue
            else:
                return f"Error {response.status_code}: {response.text}"
                
        except Exception as e:
            print(f"Request error: {e}")
            time.sleep(1)
            continue

    return "Failed to get evolution analysis."

# --- מחלקת עזר כדי שהקוד יעבוד בלי לשנות אותו ---
class Chunk:
    def __init__(self, chunk_id, text):
        self.chunk_id = chunk_id
        self.text = text

def main():
    
    # חלוקה ל-Early ו-Late כדי לבדוק את הלוגיקה האבולוציונית של הפונקציה שלך
    
    chunk_1_text = """Title: COMMUNITY IS STRONGER THAN CANCER DAY 2023; Congressional Record Vol. 169, No. 116
From the Congressional Record Online through the Government Publishing Office [www.gpo.gov]
COMMUNITY IS STRONGER THAN CANCER DAY 2023
HON. DEBBIE WASSERMAN SCHULTZ
Ms. WASSERMAN SCHULTZ. Mr. Speaker, I rise today to join with people
across America who marked June 28, 2023 as Community Is Stronger Than
Cancer Day. For those whose lives have been impacted by cancer, the community
that is found in this shared experience is an opportunity for hope, for
comfort, and even joy. Toward this end, I want to recognize the Cancer
Support Community, a global non-profit network of 190 locations,
including Gilda's Clubs, hospital and clinic partnerships, and
satellite locations that deliver more than $50 million in free support
and navigation services to patients and families. They also provide a
compassionate and engaged community for people who experienced a cancer
diagnosis themselves or who support a loved one. The Cancer Support Community reaches more than one million people
each year, providing both in-person and virtual services as well as
educational and digital resources. My district is home to those who can
seek support and programming at one of two Cancer Support Community
locations in Florida, and I ask all my colleagues to review where there
currently are locations in or near their districts and to learn more
about the important services being provided in their communities. The Cancer Support Community also maintains a toll-free national
Cancer Support Helpline. It is crucial to note that all their services
are led by licensed healthcare professionals and resource experts.
Additionally, their headquarter staff conducts cutting-edge research
that sheds light on the realities of coping with a cancer diagnosis,
including the emotional, psychological, and financial impact. so I
encourage all my colleagues to learn more about them. There are more than 18 million cancer patients and survivors in the
United States, and for every one cancer patient, there are at least
three others impacted by the diagnosis--spouse, partner, child,
caregiver, and employer. A cancer diagnosis is life changing and, for
some, can be overwhelming and distressing. I applaud the Cancer Support
Community and its network partners for the extraordinary work they do
every day easing the burdens of cancer and eliminating barriers to
care. I commend their unwavering commitment to ensuring that all people
impacted by cancer know that there are individuals and organizations
that are ready to support them as they cope with the challenges that
come with this complex diagnosis. As the Cancer Support Community emphasizes: ``While we have witnessed
many significant advances in treating this devastating disease, nothing
takes the place of the power, inspiration, companionship, and
connection that comes from community. We are here to provide relevant
and highly personalized support when and where it is needed most
because Community Is Stronger Than Cancer.''
From the Congressional Record Online through the Government Publishing Office [www.gpo.gov]
CONGRATULATING CORPORATE IMAGE ON THEIR 30TH ANNIVERSARY
HON. DIANA HARSHBARGER
Mrs. HARSHBARGER. Mr. Speaker, I rise today to recognize a Tennessee
business as it celebrates its 30th anniversary. This achievement is a
testament to the hard work and dedication of the entire team at
Corporate Image in Bristol, Tennessee. For three decades, Corporate Image has been a leading provider of
public and media relations services to businesses on the local,
regional, and national levels. They have assisted countless companies
establish and grow their brand identity, develop effective
communications strategies, and increase their visibility. The company's
commitment to quality has earned it a reputation as a trusted partner
for businesses looking to enhance their reputation and achieve long-
term communications success. The team at Corporate Image has steadily
delivered exceptional results, and their dedication to their clients
has been unwavering. Corporate Image has consistently demonstrated its ability to adapt to
the ever-changing needs of businesses in today's environment."""

    chunk_2_text = """The company's commitment to quality has earned it a reputation as a trusted partner
for businesses looking to enhance their reputation and achieve long-term communications success.
The team at Corporate Image has steadily delivered exceptional results, and their dedication to their clients
has been unwavering. Corporate Image has consistently demonstrated its ability to adapt to
the ever-changing needs of businesses in today's environment. They have embraced new technologies and innovation, while also maintaining a focus
on the core principles that have guided their business for 30 years. As we celebrate Corporate Image's 30th anniversary, we should take a
moment to reflect on the contributions this company has made to the business community.
I want to extend my heartfelt congratulations to Jon Lundberg and his entire team at Corporate Image.
Their hard work, expertise, and commitment to excellence have been key to their success, and they have
certainly set an example for other businesses to follow. Their willingness to embrace new ideas and technologies will ensure that they
remain a leader in the marketing industry and continue to thrive for years to come.
I ask that my colleagues join me in congratulating the Corporate Image team on their 30th anniversary.
I wish them continued success in all their future endeavors.
From the Congressional Record Online through the Government Publishing Office [www.gpo.gov]
CONGRATULATING NHC KINGSPORT FOR 2022 NHC CENTER OF THE YEAR
HON. DIANA HARSHBARGER
Mrs. HARSHBARGER. Mr. Speaker, I rise today to recognize and commend
NHC Kingsport on their outstanding achievements in healthcare and their
recent recognition as the 2022 NHC Center of the Year.
Located in Kingsport, Tennessee, NHC Kingsport is a vital 60-bed
healthcare center that has steadfastly served the Tri-Cities and
surrounding communities for many years. Their commitment to quality
patient care and family support is a testament to the positive impact
that dedicated healthcare professionals can make in the lives of those
they serve. Under the proficient leadership of Administrator Debbie Hubbard and
Director of Nursing Eva Grapperhaus, NHC Kingsport has flourished. Ms.
Hubbard and Ms. Grapperhaus have held their respective positions since
the center's opening in 2014. Notably, Ms. Hubbard was also recognized
as the 2019 NHC Administrator of the Year.
NHC Kingsport is distinguished as a CMS 5-Star Center, a prestigious
rating that signifies it ranks within the top 10 percent of all skilled
nursing centers. Additionally, in 2022, NHC Kingsport was recognized as
an NHC 5-Star Center of Excellence, boasting an exemplary Consumer View
score of 96.82 percent, a Patient Care Component of 96.75 percent, a
pressure ulcer rate of only 0.59 percent, and a net patient
satisfaction score of 68 percent. Their consistency in excellence is demonstrated by NHC Kingsport
being ranked among the top 10 in the NHC Center of the Year rankings
for the past six years. Moreover, they have received numerous NHC Honor
Club and Legends Awards, further solidifying their standing as a
paragon in the healthcare industry. National HealthCare Corporation, based in Murfreesboro, TN, operates
68 healthcare centers and nearly 8,500 beds across the Nation. Their
comprehensive services include assisted living, home care, hospice,
mentai health, pharmacy, insurance, and management services.
I ask my colleagues to join me in congratulating NHC Kingsport for
their unwavering dedication to providing the highest level of care to
their patients. Their achievements are not only a credit to the city of
Kingsport, but a shining example for healthcare centers throughout our
great Nation.
From the Congressional Record Online through the Government Publishing Office [www.gpo.gov]
HONORING ATR2 RONALD J. OTTENWESS
HON. DIANA HARSHBARGER
Mrs. HARSHBARGER. Mr. Speaker, Ronald J. Ottenwess enlisted in the
U.S. Navy in May 1966. After recruit training at Great Lakes Naval
Training Center, he trained as an Aviation Electronics Technician at
Naval Air Technical Training Center in Memphis, Tennessee.
Then-AT Airman Ottenwess arrived in Rota, Spain in May 1967--just in
time for Israel's Six Day War--for his assignment to Fleet Air
Reconnaissance Squadron Two (VQ-2)."""

    chunk_3_text = """She married her husband Alfonso in 1989 and had 3 amazing children.
Her family was the center of her life--she was an amazing
mother that did whatever she could to be there for her children and
husband at every moment of their lives. Aside from being an amazing mother and wife, Valles was also an
accomplished vocalist. Known as the ``The Queen of Mariachi Music,''
she captured the attention of countless North Texas Fans, from singing
National Anthem at a Mavericks or Texas Rangers game to singing at
local weddings. Her amazing voice led her to showcase her skills in
various productions. She was cast in leading roles in ``The King and
I,'' as well as in independent productions like ``Fiestas de Mi
Tierra'' and Spanish movies such as ``Yo Soy El Hijo del Michoacano.''
Her vocal skills also landed her numerous opportunities to record
voice-overs and jingles for TV and Radio. She additionally hosted
Channel 44's ``Happy Sunday's Variety Show'' and ``Salud Para Usted y
Su Familia.'' Valles gave back to the community in numerous other ways. Norma's
vocals left such a deep impact that David Albert wanted to become her
mentor. From that day on, they were a team that just kept on giving.
David brought her into the studio, and they went on to record nine
albums plus Si Se Puede CDS to help the Spanish community to learn
English. He asked her to manage Casita Tex-Mex Bar & Grill, which she
later became the owner of and is still standing today. Casita Tex-Mex
Bar & Grill had a love for veterans--from welcoming the troops at DFW
to now honoring veterans every First Tuesday of the month at Casita
Tex-Mex. Valles also had worked at the Oak Cliff Chamber of Commerce
and spent her life involved in various organizations, ranging from the
Irving Hispanic Chamber, H100, North Dallas of Commerce, HWNT, Dallas
St. Patrick's Day parade, Dia De Los Muertos and so many more. She also
remodeled and fixed up homes across North Texas, later starting her own
business and becoming General Contractor for her business NV
Contractors. Valles will be forever missed by loved ones.
She blessed the lives of many and will continue to bless us with her music and businesses.
From the Congressional Record Online through the Government Publishing Office [www.gpo.gov]
HONORING THE NATIONAL ASSOCIATION OF BLACKS IN CRIMINAL JUSTICE'S 50TH ANNIVERSARY
HON. ROBERT C. ``BOBBY'' SCOTT
Mr. SCOTT of Virginia. Mr. Speaker, I rise today to honor and
recognize the National Association of Blacks in Criminal Justice
(NABCJ) on their 50th Anniversary.
The mission of the National Association of Blacks in Criminal Justice
is to act upon the needs, concerns, and contributions of African
Americans and other people of color as they relate to the
administration of equal justice. This milestone of NABCJ advancing
justice for 50 years signifies decades of positive change and a
commitment to criminal justice as a fundamental aspect of our
democracy. At the heart of NABCJ is its dedicated members, who consist of
criminal justice professionals including those in law enforcement,
corrections, courts, social services', academia as well as religious
and community leaders. They work diligently to research relevant
legislation and focus attention on improving law enforcement and our
criminal justice system through evidence-based policies. Their strong
commitment to respect the dignity of all humans contributes to the
NABCJ's rich history of excellence and integrity.
The 400 Years African American History Commission, created by
Congress in 2017, has been heavily engaged with NABCJ through 11
student chapters of the organization at Historically Black Colleges and
Universities. Additionally, the National Alliance of Faith and Justice,
first formed as a committee of the NABCJ, was founded and has
successfully grown over the last 20 years. The establishment of these
relationships have helped pave the way for African Americans and other
people of color to receive equal justice under the law."""

    chunk_4_text = """In 1988, Robb Lally headed to San Diego to work with the founder of
the Alpha Project, a non-profit human services organization that serves
over 4,000 men, women and children daily. As director, he empowered
individuals, families and communities by providing work, recovery and
support services to those who were motivated to change their lives and
achieve self-sufficiency. During his tenure, Robb was instrumental in
the overall mission and growth of the organization and had assisted
with over 1,000 apartment units that were preserved and created.
On June 7, 2023, Robb's life and legacy were honored at a Celebration
of Life in San Diego, CA. On behalf of the residents of California's
52nd Congressional District, I would like to express my deepest
condolences to the family of Robb Lally. His legacy is felt, and his
presence will be greatly missed."""

    #  המרה לאובייקטים שהפונקציה דורשת
    early_chunks = [Chunk(1, chunk_1_text), Chunk(2, chunk_2_text)]
    late_chunks = [Chunk(3, chunk_3_text), Chunk(4, chunk_4_text)]

    #הגדרת שאילתה לבדיקה
    query = "What are the community support initiatives mentioned and how have they evolved?"

    print("Sending request to Gemini API...")
    final_answer = run_evolution_llm(query, early_chunks, late_chunks)
    
    #  הדפסת התוצאה
    print("\n--- FINAL EVOLUTION ANALYSIS ---")
    print(final_answer)

if __name__ == "__main__":
    main()
