use rand::prelude::IndexedRandom;
use reqwest::header;
use reqwest::header::{HeaderMap, HeaderValue};

fn generate_domain_from_name(company_name: &str) -> String {
    company_name
        .to_lowercase() // Convert to lowercase
        .replace(" ", "") // Remove spaces
        .replace("'", "") // Remove apostrophes
        .replace("&", "and") // Replace & with "and"
}

pub fn generate_headers() -> HeaderMap {
    let mut headers: HeaderMap = HeaderMap::new();
    let (company_name, email): (String, String) = generate_company_name_and_email();
    let both = format!("{} {}", company_name, email);

    headers.insert(header::USER_AGENT, HeaderValue::from_str(&*both).unwrap());
    headers.insert(header::HOST, HeaderValue::from_str("www.sec.gov").unwrap());

    headers
}

fn generate_company_name_and_email() -> (String, String) {
    let company_prefixes = vec![
        "Tech",
        "Global",
        "Future",
        "Net",
        "Data",
        "Sky",
        "Bright",
        "Prime",
        "Green",
        "Cloud",
        "Quantum",
        "Innovative",
        "Smart",
        "Blue",
        "Secure",
        "NextGen",
    ];
    let company_suffixes = vec![
        "Solutions",
        "Corp",
        "Systems",
        "Holdings",
        "Networks",
        "Consulting",
        "Group",
        "Technologies",
        "Ventures",
        "Partners",
        "Industries",
        "Services",
        "Enterprises",
    ];

    let mut rng = rand::thread_rng();

    let prefix = company_prefixes.choose(&mut rng).unwrap();
    let suffix = company_suffixes.choose(&mut rng).unwrap();
    let company_name = format!("{} {}", prefix, suffix);

    let domain_name = generate_domain_from_name(&company_name);

    let email = format!("admin@{}.com", domain_name);

    (company_name, email)
}