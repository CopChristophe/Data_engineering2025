from bs4 import BeautifulSoup
import pandas as pd

def parse_html(html_content, url):
    """Extracts data from HTML content based on the URL."""
    if url == "https://en.wikipedia.org/wiki/List_of_fallacies":
        relevant_h2_categories = {"Formal fallacies", "Informal fallacies"}
    elif url == "https://en.wikipedia.org/wiki/List_of_cognitive_biases":
        relevant_h2_categories = {"Belief, decision-making and behavioral", "Memory"}
    else:
        raise ValueError(f"Unknown URL: {url}")

    extracted_data = []
    soup = BeautifulSoup(html_content, "html.parser")

    for h2_tag in soup.find_all('h2'):
        category_title = h2_tag.get_text(strip=True)
        
        if category_title in relevant_h2_categories:
            section_li_tags = h2_tag.find_all_next('li')

            for li_tag in section_li_tags:
                a_tag = li_tag.find('a', title=True)
                if a_tag:
                    bias_name = a_tag['title']
                    h4_tag = li_tag.find_previous('h4')
                    h3_tag = li_tag.find_previous('h3')
                    h2_tag = li_tag.find_previous('h2')

                    extracted_data.append({
                        "Bias Name": bias_name,
                        "H4 Tag": h4_tag.get_text(strip=True) if h4_tag else None,
                        "H3 Tag": h3_tag.get_text(strip=True) if h3_tag else None,
                        "H2 Tag": h2_tag.get_text(strip=True) if h2_tag else None,
                    })

    df = pd.DataFrame(extracted_data)
    df = df[df['H2 Tag'].isin(relevant_h2_categories)].drop_duplicates()
    
    return df
