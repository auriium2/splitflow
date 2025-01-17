//TODO: use inference

use regex::Regex;
use serde::{Deserialize, Serialize};

pub(crate) struct RSSPhaseOneDetector<const N: usize> {
    synonyms: [&'static str; N],
    regex: Vec<Regex>
}

#[derive(Clone, Deserialize, Serialize, Debug, Hash, PartialEq, Eq)]
pub struct RssPresence(pub bool, bool, bool); 

impl RSSPhaseOneDetector<22> {


    //please run this in rayon
    //why regex slow ._.
    //please stop running in debug profile
    
    
    pub fn detect_rss_potential(&self, filing_text: &str) -> RssPresence {
        let lower = filing_text.to_lowercase();


        // Check if we have a synonym for reverse stock split or a share consolidation
        let has_synonym = self.synonyms.iter().any(|syn| lower.contains(syn.to_lowercase().as_str()));

        // Check ratio
        let has_ratio = self.regex
            .iter()
            .any(|regex| regex.is_match(&lower));

        // Decide
        RssPresence(has_synonym || has_ratio, has_synonym, has_ratio)
    }
    
    pub fn new() -> RSSPhaseOneDetector<22> {
        let patterns = [
            "reverse ADS split",
            "ADS split",
            "reverse stock split",
            "reverse split",
            "share consolidation",
            "consolidation of shares",
            "stock consolidation",
            "consolidation of outstanding shares",
            "combination of shares",
            "reverse share split",
            "reverse share consolidation",
            "share combination",
            "stock combination",
            "reverse share combination",
            "share recapitalization",
            "reverse recapitalization",
            "stock recapitalization",
            "share rollback",
            "stock rollback",
            "reverse split of the company's capital",
            "consolidation of the company's share capital",
            "reverse stock combination",
        ];

        let ratio_patterns: [&str; 4] = [
            r"(\d+)\s*-\s*for\s*-\s*(\d+)",         // 1-for-10
            r"(\d+)\s+for\s+(\d+)",                 // 1 for 10
            r"(one|two|three|four|five|six|seven|eight|nine|ten)\s+for\s+(one|two|three|four|five|six|seven|eight|nine|ten)", // spaces
            r"(one|two|three|four|five|six|seven|eight|nine|ten)\s*-\s*for\s*-\s*(one|two|three|four|five|six|seven|eight|nine|ten)", //hypens
        ];

        // Compile them all
        let compiled_regexes: Vec<Regex> = ratio_patterns
            .iter()
            .map(|p| Regex::new(p).unwrap())
            .collect();
        
        RSSPhaseOneDetector {
            synonyms: patterns,
            regex: compiled_regexes,
        }

        
    }
}

#[cfg(test)]
mod tests {
    use crate::scrape::rss_presence::RSSPhaseOneDetector;

    #[test]
    fn test_detect_6k_one() {
        
        
        
        let yap = r#"
        6-K 1 f6k_011025.htm FORM 6-K
 
UNITED STATES
SECURITIES AND EXCHANGE COMMISSION
Washington, D.C. 20549

FORM 6-K

REPORT OF FOREIGN PRIVATE ISSUER PURSUANT TO RULE 13a-16 OR 15d-16 UNDER THE SECURITIES EXCHANGE ACT OF 1934

For the month of January 2025

Commission File Number: 001-39950

Evaxion Biotech A/S
(Exact Name of Registrant as Specified in Its Charter)

 
Dr. Neergaards Vej 5f
DK-2970 Hoersholm
Denmark
(Address of principal executive offices)

 
 
Indicate by check mark whether the registrant files or will file annual reports under cover of Form 20-F or Form 40-F.

Form 20-F ☒     Form 40-F ☐

INCORPORATION BY REFERENCE
 
This report on Form 6-K shall be deemed to be incorporated by reference in Evaxion Biotech A/S’s registration statements on Form S-8 (File No. 333-255064), on Form F-3 (File No. 333-265132), on Form F-1, as amended (File No. 333-266050), Form F-1 (File No. 333-276505), Form F-1 (File No. 333-279153), and Form F-1 (File No. 333-283304), including any prospectuses forming a part of such registration statements and to be a part thereof from the date on which this report is filed, to the extent not superseded by documents or reports subsequently filed or furnished.
 

 

Material Modification to Rights of Security Holders.
 
The Board of Directors of Evaxion Biotech A/S (the “Company”) has approved a change in the ratio of its American Depositary Shares (“ADSs”) to its ordinary shares, DKK 1 nominal value (the “ADS Ratio”), from the current one (1) ADS representing ten (10) ordinary shares to a new ADS Ratio of one (1) ADS representing fifty (50) ordinary shares (the “ADS Ratio Change”). The ADS Ratio Change is now expected to become effective on or about January 14, 2025, U.S. Eastern Time (the “Effective Date”). Previously, the Company planned for the ADS ratio change to become effective on January 13, 2025, but has extended the date by one day due to the recent announcement that Nasdaq Capital Markets will be closed on January 9, 2025 in order to observe the passing of President Jimmy Carter.
 
For the Company's ADS holders, the change in the ADS Ratio will have the same effect as a one-for-five reverse ADS split and is intended to further support the liquidity in the Company’s ADSs. On the Effective Date, registered holders of the Company’s ADSs held in certificated form will be required on a mandatory basis to surrender their certificated ADSs to The Bank of New York Mellon, the depositary bank (the “Depositary”), for cancellation and will receive one (1) new ADS in exchange for every five (5) existing ADSs then-held. Holders of uncertificated ADSs in the Direct Registration System (DRS) and The Depository Trust Company (DTC) will have their ADSs automatically exchanged and need not take any action. The exchange of every five (5) then-held (existing) ADSs for one (1) new ADS will occur automatically at the Effective Date, with the then-held ADSs being cancelled and new ADSs being issued by the depositary bank. The Company’s ADSs will continue to be traded on The Nasdaq Capital Market under the ticker symbol “EVAX.”
 
No fractional new ADSs will be issued in connection with the change in the ADS Ratio. Instead, fractional entitlements to new ADSs will be aggregated and sold by the Depositary and the net cash proceeds from the sale of the fractional ADS entitlements (after deduction of fees, taxes and expenses) will be distributed to the applicable ADS holders by the Depositary.
 
As a result of the ADS Ratio Change, the ADS trading price is expected to increase proportionally, although the Company can give no assurance that the ADS trading price after the ADS Ratio Change will be proportionally equal to or greater than the previous’ ADS trading price prior to the change or that the Ratio Change will have any effect on the liquidity in the Company.
On January 10, 2025, the Registrant issued a press release, a copy of which is attached hereto as Exhibit 99.1 and is incorporated herein by reference.

Exhibit No.	 	Description
 	 	 
99.1	 	Press Release dated January 10, 2025 for ADS Ratio Change
SIGNATURES

Pursuant to the requirements of the Securities Exchange Act of 1934, the registrant has duly caused this report to be signed on its behalf by the undersigned, thereunto duly authorized.

 	Evaxion Biotech A/S
 	 	
 	 	
Date: January 10, 2025	By:	/s/ Christian Kanstrup
 	 	Name: Christian Kanstrup
 	 	Title:   Chief Executive Officer
 
 
        "#;
        
        assert_eq!(RSSPhaseOneDetector::new().detect_rss_potential(yap).0, true);
    }
    
    #[test]
    fn test_detect_8k_two() {
        let yap = r#"
         
Item 1.01 Entry into a Material Definitive Agreement.
 
On January 7, 2025, XTI Aerospace, Inc., a Nevada corporation (the “Company”), filed a certificate of amendment (the “Reverse Stock Split Amendment”) to its Restated Articles of Incorporation, as amended (the “Articles of Incorporation”), with the Secretary of State of the State of Nevada to effect a reverse stock split of its outstanding common stock, par value $0.001 per share (the “Common Stock”), at a ratio of 1-for-250, effective as of 12:01 a.m., Eastern Time, on January 10, 2025 (the “Reverse Stock Split”).
 
On January 7, 2025, the Company entered into a Placement Agency Agreement (the “Agreement”) with ThinkEquity LLC (the “Placement Agent”), pursuant to which the Company agreed to issue and sell directly to various investors, in a best efforts public offering (the “Offering”), an aggregate of 1,454,546 shares of Common Stock on a post-Reverse Stock Split basis (363,636,364 shares of Common Stock on a pre-Reverse Stock Split basis) (the “Shares”), at an offering price of $13.75 per Share on a post-Reverse Stock Split basis ($0.055 per Share on a pre-Reverse Stock Split basis). The Company is expected to receive gross proceeds of approximately $20 million in connection with the Offering, before deducting placement agent fees and other offering expenses payable by the Company. The Offering is expected to close on January 10, 2025.
 
As part of its compensation for acting as placement agent for the Offering, the Company also agreed to issue to the Placement Agent, warrants (the “Placement Agent Warrants”) to purchase 72,727 shares of Common Stock on a post-Reverse Stock Split basis (18,181,818 shares of Common Stock on a pre-Reverse Stock Split basis) (the “Placement Agent Warrant Shares”). The Placement Agent Warrants are exercisable commencing January 10, 2025, expire January 8, 2030 and have an exercise price of $17.1875 per share on a post-Reverse Stock Split basis ($0.06875 per share on a pre-Reverse Stock Split basis).
 
The Reverse Stock Split is a condition to the closing of the Offering. Accordingly, the Shares and the Placement Agent Warrants issued at the closing of the Offering will be issued on a post-Reverse Stock Split basis.
 
The Shares, the Placement Agent Warrants and the Placement Agent Warrant Shares were offered and sold pursuant to a registration statement on Form S-3 (File No. 333-279901), which was filed with the Securities and Exchange Commission (the “Commission”) on May 31, 2024, as amended on June 14, 2024, and was declared effective by the Commission on June 18, 2024 (the “Registration Statement”), the base prospectus included therein, as amended and supplemented by the prospectus supplement dated January 7, 2025. A copy of the opinion of Mitchell Silberberg & Knupp LLP relating to the legality of the issuance and sale of the Shares, the Placement Agent Warrants and the Placement Agent Warrant Shares is attached as Exhibit 5.1 hereto.
 
Pursuant to the terms of the Agreement, the Company agreed to pay the Placement Agent a cash fee equal to 7.0% of the gross proceeds of the Offering and to reimburse the Placement Agent for certain of its expenses in an aggregate amount up to $175,000. The Company further agreed not to issue, enter into any agreement to issue or announce the issuance or proposed issuance of, any shares of Common Stock or any securities convertible into or exercisable or exchangeable for shares of Common Stock or file any registration statement or prospectus, or any amendment or supplement thereto for a period of 30 days from January 7, 2025, subject to certain exceptions. Additionally, each of the directors and officers of the Company, pursuant to lock-up agreements (the “Lock-Up Agreements”), agreed not to sell or transfer any of the Company securities which they hold, subject to certain exceptions, for a period of 90 days from January 7, 2025.
 
The representations, warranties and covenants contained in the Agreement were made solely for the benefit of the parties to the Agreement. In addition, such representations, warranties and covenants (i) are intended as a way of allocating the risk between the parties to the Agreement and not as statements of fact, and (ii) may apply standards of materiality in a way that is different from what may be viewed as material by stockholders of, or other investors in, the Company. Moreover, information concerning the subject matter of the representations and warranties may change after the date of the Agreement, which subsequent information may or may not be fully reflected in public disclosures."#;

        assert_eq!(RSSPhaseOneDetector::new().detect_rss_potential(yap).0, true);
    }
    
    
}