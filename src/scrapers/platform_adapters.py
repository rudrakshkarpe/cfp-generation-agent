"""
Platform-specific adapters for different conference systems
"""
import aiohttp
import asyncio
import json
import re
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from datetime import datetime

class BaseConferenceAdapter(ABC):
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip('/')
        self.session = None
    
    @abstractmethod
    async def extract_talk_urls(self) -> List[str]:
        """Extract all individual talk URLs"""
        pass
    
    @abstractmethod
    async def parse_talk_page(self, talk_url: str) -> Dict[str, Any]:
        """Parse individual talk page content"""
        pass

class SchedAdapter(BaseConferenceAdapter):
    """Adapter for Sched.com platforms (KubeCon, DockerCon, etc.)"""
    
    async def extract_talk_urls(self) -> List[str]:
        """Extract talk URLs from Sched.com"""
        
        # Try multiple Sched endpoints
        endpoints = [
            "/list/descriptions/",
            "/directory/",
            "/"
        ]
        
        all_urls = set()
        
        async with aiohttp.ClientSession() as session:
            for endpoint in endpoints:
                try:
                    url = f"{self.base_url}{endpoint}"
                    async with session.get(url, timeout=15) as response:
                        if response.status == 200:
                            html = await response.text()
                            urls = self._extract_sched_urls(html)
                            all_urls.update(urls)
                            
                            if len(urls) > 0:  # If we found URLs, prioritize this endpoint
                                break
                                
                except Exception as e:
                    print(f"Error fetching {endpoint}: {str(e)}")
                    continue
        
        return list(all_urls)
    
    def _extract_sched_urls(self, html: str) -> List[str]:
        """Extract Sched event URLs from HTML"""
        soup = BeautifulSoup(html, 'html.parser')
        urls = set()
        
        # Look for event links
        for link in soup.find_all('a', href=True):
            href = link['href']
            if href.startswith('event/') or '/event/' in href:
                if not href.startswith('http'):
                    full_url = f"{self.base_url}/{href.lstrip('/')}"
                else:
                    full_url = href
                urls.add(full_url)
        
        return list(urls)
    
    async def parse_talk_page(self, talk_url: str) -> Dict[str, Any]:
        """Parse Sched talk page"""
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(talk_url, timeout=10) as response:
                    if response.status != 200:
                        return self._empty_talk_data(talk_url)
                    
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    return {
                        'title': self._extract_sched_title(soup),
                        'description': self._extract_sched_description(soup),
                        'speaker': self._extract_sched_speakers(soup),
                        'category': self._extract_sched_category(soup),
                        'time_slot': self._extract_sched_time(soup),
                        'room': self._extract_sched_room(soup),
                        'url': talk_url,
                        'platform': 'sched',
                        'crawled_at': datetime.utcnow().isoformat()
                    }
                    
        except Exception as e:
            print(f"Error parsing Sched talk {talk_url}: {str(e)}")
            return self._empty_talk_data(talk_url)
    
    def _extract_sched_title(self, soup: BeautifulSoup) -> str:
        selectors = [
            'span.event a.name',
            'h1.event-title',
            '.event-name',
            'h1',
            '.session-title'
        ]
        
        for selector in selectors:
            elem = soup.select_one(selector)
            if elem and elem.get_text().strip():
                return elem.get_text().strip()
        
        return "Unknown Title"
    
    def _extract_sched_description(self, soup: BeautifulSoup) -> str:
        selectors = [
            '.tip-description',
            '.event-description',
            '.description',
            '.abstract'
        ]
        
        for selector in selectors:
            elem = soup.select_one(selector)
            if elem and elem.get_text().strip():
                return elem.get_text().strip()
        
        return "No description available"
    
    def _extract_sched_speakers(self, soup: BeautifulSoup) -> str:
        speakers = []
        
        # Try different speaker selectors
        speaker_selectors = [
            '.sched-event-details-roles h2 a',
            '.speaker-name',
            '.presenters a',
            '.event-speakers a'
        ]
        
        for selector in speaker_selectors:
            speaker_elems = soup.select(selector)
            for elem in speaker_elems:
                speaker_name = elem.get_text().strip()
                if speaker_name and speaker_name not in speakers:
                    speakers.append(speaker_name)
        
        return ' & '.join(speakers) if speakers else 'Unknown Speaker'
    
    def _extract_sched_category(self, soup: BeautifulSoup) -> str:
        selectors = [
            '.sched-event-type a',
            '.event-category',
            '.track',
            '.category'
        ]
        
        for selector in selectors:
            elem = soup.select_one(selector)
            if elem and elem.get_text().strip():
                return elem.get_text().strip()
        
        return "Uncategorized"
    
    def _extract_sched_time(self, soup: BeautifulSoup) -> str:
        selectors = [
            '.sched-event-details-timeandplace',
            '.event-time',
            '.time-slot'
        ]
        
        for selector in selectors:
            elem = soup.select_one(selector)
            if elem:
                time_text = elem.get_text().split('\n')[0].strip()
                if time_text:
                    return time_text
        
        return "Unknown Time"
    
    def _extract_sched_room(self, soup: BeautifulSoup) -> str:
        selectors = [
            '.sched-event-details-timeandplace a',
            '.event-location',
            '.room'
        ]
        
        for selector in selectors:
            elem = soup.select_one(selector)
            if elem and elem.get_text().strip():
                return elem.get_text().strip()
        
        return "Unknown Room"
    
    def _empty_talk_data(self, url: str) -> Dict[str, Any]:
        return {
            'title': 'Failed to Parse',
            'description': 'Error occurred while parsing this talk',
            'speaker': 'Unknown',
            'category': 'Error',
            'time_slot': 'Unknown',
            'room': 'Unknown',
            'url': url,
            'platform': 'sched',
            'crawled_at': datetime.utcnow().isoformat()
        }

class SessionizeAdapter(BaseConferenceAdapter):
    """Adapter for Sessionize.com platforms"""
    
    async def extract_talk_urls(self) -> List[str]:
        """Extract talks from Sessionize API or web scraping"""
        
        # Try to find sessionize ID from URL
        sessionize_id = self._extract_sessionize_id()
        
        if sessionize_id:
            # Try API first
            talks_data = await self._fetch_from_api(sessionize_id)
            if talks_data:
                return talks_data  # Return talk data directly for API
        
        # Fallback to web scraping
        return await self._scrape_sessionize_web()
    
    def _extract_sessionize_id(self) -> Optional[str]:
        """Extract Sessionize event ID from URL"""
        # Sessionize URLs often have format: https://sessionize.com/event-name/
        parsed = urlparse(self.base_url)
        path_parts = parsed.path.strip('/').split('/')
        
        if len(path_parts) > 0 and path_parts[0]:
            return path_parts[0]
        
        return None
    
    async def _fetch_from_api(self, sessionize_id: str) -> Optional[List[Dict]]:
        """Fetch from Sessionize public API"""
        
        api_url = f"https://sessionize.com/api/v2/{sessionize_id}/view/Sessions"
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(api_url, timeout=15) as response:
                    if response.status == 200:
                        data = await response.json()
                        return self._parse_sessionize_api_data(data)
        except Exception as e:
            print(f"Error fetching Sessionize API: {str(e)}")
        
        return None
    
    def _parse_sessionize_api_data(self, sessions_data: List[Dict]) -> List[Dict]:
        """Parse Sessionize API response into talk data"""
        talks = []
        
        for day in sessions_data:
            for room in day.get('rooms', []):
                for session in room.get('sessions', []):
                    if not session.get('isPlenumSession', False):  # Skip plenary sessions
                        talk_data = {
                            'title': session.get('title', 'Unknown Title'),
                            'description': session.get('description', 'No description available'),
                            'speaker': ' & '.join([s.get('name', '') for s in session.get('speakers', [])]),
                            'category': ', '.join([c.get('name', '') for c in session.get('categories', [])]),
                            'time_slot': f"{session.get('startsAt', '')} - {session.get('endsAt', '')}",
                            'room': room.get('name', 'Unknown Room'),
                            'url': f"{self.base_url}/session/{session.get('id', '')}",
                            'platform': 'sessionize',
                            'crawled_at': datetime.utcnow().isoformat()
                        }
                        talks.append(talk_data)
        
        return talks
    
    async def _scrape_sessionize_web(self) -> List[str]:
        """Fallback web scraping for Sessionize"""
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.base_url, timeout=15) as response:
                    if response.status != 200:
                        return []
                    
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    urls = set()
                    for link in soup.find_all('a', href=True):
                        href = link['href']
                        if '/session/' in href or 'session' in href.lower():
                            if not href.startswith('http'):
                                href = urljoin(self.base_url, href)
                            urls.add(href)
                    
                    return list(urls)
                    
        except Exception as e:
            print(f"Error scraping Sessionize: {str(e)}")
            return []
    
    async def parse_talk_page(self, talk_data: Any) -> Dict[str, Any]:
        """For Sessionize, data might already be parsed from API"""
        
        if isinstance(talk_data, dict) and 'platform' in talk_data:
            return talk_data  # Already parsed from API
        
        # If it's a URL, scrape it
        if isinstance(talk_data, str):
            return await self._scrape_sessionize_talk(talk_data)
        
        return self._empty_talk_data(str(talk_data))
    
    async def _scrape_sessionize_talk(self, talk_url: str) -> Dict[str, Any]:
        """Scrape individual Sessionize talk page"""
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(talk_url, timeout=10) as response:
                    if response.status != 200:
                        return self._empty_talk_data(talk_url)
                    
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    return {
                        'title': self._extract_generic_title(soup),
                        'description': self._extract_generic_description(soup),
                        'speaker': self._extract_generic_speakers(soup),
                        'category': self._extract_generic_category(soup),
                        'time_slot': self._extract_generic_time(soup),
                        'room': self._extract_generic_room(soup),
                        'url': talk_url,
                        'platform': 'sessionize',
                        'crawled_at': datetime.utcnow().isoformat()
                    }
                    
        except Exception as e:
            print(f"Error scraping Sessionize talk {talk_url}: {str(e)}")
            return self._empty_talk_data(talk_url)
    
    def _extract_generic_title(self, soup: BeautifulSoup) -> str:
        selectors = ['h1', '.session-title', '.title', 'h2']
        for selector in selectors:
            elem = soup.select_one(selector)
            if elem and elem.get_text().strip():
                return elem.get_text().strip()
        return "Unknown Title"
    
    def _extract_generic_description(self, soup: BeautifulSoup) -> str:
        selectors = ['.description', '.abstract', '.session-description', 'p']
        for selector in selectors:
            elem = soup.select_one(selector)
            if elem and len(elem.get_text().strip()) > 50:  # Ensure substantial content
                return elem.get_text().strip()
        return "No description available"
    
    def _extract_generic_speakers(self, soup: BeautifulSoup) -> str:
        speakers = []
        selectors = ['.speaker', '.presenter', '.author']
        for selector in selectors:
            elems = soup.select(selector)
            for elem in elems:
                speaker = elem.get_text().strip()
                if speaker and speaker not in speakers:
                    speakers.append(speaker)
        return ' & '.join(speakers) if speakers else 'Unknown Speaker'
    
    def _extract_generic_category(self, soup: BeautifulSoup) -> str:
        selectors = ['.category', '.track', '.tag']
        for selector in selectors:
            elem = soup.select_one(selector)
            if elem and elem.get_text().strip():
                return elem.get_text().strip()
        return "Uncategorized"
    
    def _extract_generic_time(self, soup: BeautifulSoup) -> str:
        selectors = ['.time', '.schedule', '.when']
        for selector in selectors:
            elem = soup.select_one(selector)
            if elem and elem.get_text().strip():
                return elem.get_text().strip()
        return "Unknown Time"
    
    def _extract_generic_room(self, soup: BeautifulSoup) -> str:
        selectors = ['.room', '.location', '.where']
        for selector in selectors:
            elem = soup.select_one(selector)
            if elem and elem.get_text().strip():
                return elem.get_text().strip()
        return "Unknown Room"
    
    def _empty_talk_data(self, url: str) -> Dict[str, Any]:
        return {
            'title': 'Failed to Parse',
            'description': 'Error occurred while parsing this talk',
            'speaker': 'Unknown',
            'category': 'Error',
            'time_slot': 'Unknown',
            'room': 'Unknown',
            'url': url,
            'platform': 'sessionize',
            'crawled_at': datetime.utcnow().isoformat()
        }

class NDCAdapter(BaseConferenceAdapter):
    """Adapter for NDC conferences (dynamic support for all NDC sites)"""
    
    async def extract_talk_urls(self) -> List[str]:
        """Extract talk URLs from NDC agenda page"""
        # Build agenda URL if not already an agenda page
        if '/agenda' not in self.base_url:
            agenda_url = f"{self.base_url.rstrip('/')}/agenda"
        else:
            agenda_url = self.base_url
            
        urls = set()
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(agenda_url, timeout=15) as response:
                    if response.status != 200:
                        print(f"Failed to fetch agenda: HTTP {response.status}")
                        return []
                    
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # Find all links on the agenda page
                    for a in soup.find_all('a', href=True):
                        href = a['href']
                        # NDC pattern: /agenda/session-name/session-id
                        # Look for links that start with /agenda/ and have more than just /agenda/
                        if href.startswith('/agenda/') and href != '/agenda/' and len(href.split('/')) > 2:
                            # Build full URL
                            full_url = urljoin(self.base_url, href)
                            # Get link text for validation
                            link_text = a.get_text(strip=True)
                            # Only add if it has meaningful text (likely a talk title)
                            if link_text and len(link_text) > 5:
                                urls.add(full_url)
                    
                    print(f"Found {len(urls)} talk URLs from NDC agenda")
                    
        except Exception as e:
            print(f"Error extracting NDC talk URLs: {str(e)}")
            
        return sorted(list(urls))

    async def parse_talk_page(self, talk_url: str) -> Dict[str, Any]:
        """Parse NDC talk page with robust extraction"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(talk_url, timeout=15) as response:
                    if response.status != 200:
                        print(f"Failed to fetch talk page: HTTP {response.status}")
                        return self._empty_talk_data(talk_url)
                    
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # Extract title - try multiple selectors
                    title = self._extract_title(soup)
                    
                    # Extract speaker(s) - look for links to /speakers/
                    speakers = self._extract_speakers(soup)
                    
                    # Extract description - get the longest meaningful paragraph
                    description = self._extract_description(soup)
                    
                    # Extract metadata from page text
                    page_text = ' '.join(soup.get_text(separator=' ').split())
                    
                    # Room extraction (e.g., "Room 1", "Room A")
                    room = self._extract_room(page_text)
                    
                    # Time extraction (e.g., "09:00 - 10:00")
                    time_window = self._extract_time_window(page_text)
                    
                    # Timezone extraction (e.g., "(UTC+02)")
                    timezone = self._extract_timezone(page_text)
                    
                    # Talk type and duration (e.g., "Talk (60 min)" or "Workshop (120 min)")
                    category, duration = self._extract_type_and_duration(page_text)
                    
                    # Weekday extraction
                    weekday = self._extract_weekday(page_text)
                    
                    # Combine time slot information
                    time_slot_parts = [time_window, timezone, weekday]
                    time_slot = ' '.join(filter(None, time_slot_parts)).strip() or "Unknown Time"
                    
                    return {
                        'title': title,
                        'description': description,
                        'speaker': speakers,
                        'category': category,
                        'time_slot': time_slot,
                        'room': room,
                        'url': talk_url,
                        'platform': 'ndc',
                        'crawled_at': datetime.utcnow().isoformat()
                    }
                    
        except Exception as e:
            print(f"Error parsing NDC talk {talk_url}: {str(e)}")
            return self._empty_talk_data(talk_url)
    
    def _extract_title(self, soup: BeautifulSoup) -> str:
        """Extract talk title from various possible locations"""
        # Try all h1 elements, skip the NDC branding
        h1_elements = soup.find_all('h1')
        for h1 in h1_elements:
            title = h1.get_text(strip=True)
            # Skip if it's the NDC branding (contains "NDC{")
            if title and 'NDC{' not in title:
                return title
        
        # Try meta og:title and clean it up
        meta_title = soup.find('meta', {'property': 'og:title'})
        if meta_title and meta_title.get('content'):
            title = meta_title.get('content').strip()
            # Remove NDC suffix/prefix patterns
            title = re.sub(r'^NDC\{[^}]+\}\s*[-–]\s*', '', title, flags=re.IGNORECASE)
            title = re.sub(r'\s*[-–]\s*NDC\{[^}]+\}$', '', title, flags=re.IGNORECASE)
            title = re.sub(r'\s*[-|]\s*NDC.*$', '', title, flags=re.IGNORECASE)
            if title and title != 'NDC':
                return title
        
        # Try page title element
        title_elem = soup.find('title')
        if title_elem:
            title = title_elem.get_text(strip=True)
            # Clean up NDC branding
            title = re.sub(r'^NDC\{[^}]+\}\s*[-–]\s*', '', title, flags=re.IGNORECASE)
            title = re.sub(r'\s*[-–]\s*NDC\{[^}]+\}$', '', title, flags=re.IGNORECASE)
            title = re.sub(r'\s*[-|]\s*NDC.*$', '', title, flags=re.IGNORECASE)
            title = re.sub(r'^NDC\s+', '', title, flags=re.IGNORECASE)
            if title and title != 'NDC' and len(title) > 3:
                return title
        
        # Try to find title in specific divs or sections
        title_selectors = [
            'h2.talk-title', 'h2.session-title', 'h2.event-title',
            'div.talk-title', 'div.session-title', 'div.event-title',
            '.title', '.session-name', '.talk-name'
        ]
        for selector in title_selectors:
            elem = soup.select_one(selector)
            if elem:
                title = elem.get_text(strip=True)
                if title and 'NDC{' not in title:
                    return title
        
        # Fallback: extract from URL if available
        # URLs often contain the talk title as a slug
        current_url = soup.find('link', {'rel': 'canonical'})
        if current_url and current_url.get('href'):
            url = current_url['href']
        else:
            # Try to find URL from meta property
            meta_url = soup.find('meta', {'property': 'og:url'})
            if meta_url and meta_url.get('content'):
                url = meta_url['content']
            else:
                url = None
        
        if url and '/agenda/' in url:
            # Extract slug from URL like /agenda/talk-title-here/sessionid
            match = re.search(r'/agenda/([^/]+)/', url)
            if match:
                slug = match.group(1)
                # Convert slug to title (replace hyphens with spaces, capitalize)
                title = slug.replace('-', ' ').title()
                # Remove common words that might be in the slug
                title = re.sub(r'\b(0[a-z0-9]+)\b', '', title, flags=re.IGNORECASE).strip()
                if title and len(title) > 3:
                    return title
        
        return "Unknown Title"
    
    def _extract_speakers(self, soup: BeautifulSoup) -> str:
        """Extract speaker names from speaker profile links"""
        speakers = []
        
        # Look for links to speaker profiles
        for a in soup.find_all('a', href=True):
            href = a['href']
            if '/speakers/' in href or '/speaker/' in href:
                speaker_name = a.get_text(strip=True)
                if speaker_name and speaker_name not in speakers:
                    # Filter out non-name text
                    if len(speaker_name) < 50 and not speaker_name.startswith('http'):
                        speakers.append(speaker_name)
        
        # Fallback: look for speaker class or h3/h4 that might contain speaker name
        if not speakers:
            for elem in soup.find_all(['h3', 'h4', 'span'], class_=re.compile(r'speaker', re.I)):
                text = elem.get_text(strip=True)
                if text and len(text) < 50:
                    speakers.append(text)
                    break
        
        return ' & '.join(speakers) if speakers else 'Unknown Speaker'
    
    def _extract_description(self, soup: BeautifulSoup) -> str:
        """Extract the most meaningful description from paragraphs, excluding speaker bios"""
        
        # Bio keywords that indicate speaker biography rather than talk description
        bio_keywords = [
            'is the developer', 'is a developer', 'is the', 'is a', 'has been', 
            'worked at', 'works at', 'working at', 'years of experience', 
            'completed a phd', 'completed his', 'completed her', 'holds a',
            'is currently', 'was previously', 'has worked', 'she has', 'he has',
            'is passionate about', 'specializes in', 'focused on', 'expertise',
            'lead data scientist', 'software engineer', 'program manager',
            'developer advocate', 'technical lead', 'architect at', 'cto',
            'founder', 'co-founder', 'director', 'manager', 'consultant'
        ]
        
        # Talk description keywords that indicate actual session content
        talk_keywords = [
            'this talk', 'this session', 'we will', 'we\'ll', 'you will',
            'explore', 'discuss', 'learn', 'discover', 'demonstrate',
            'in this', 'during this', 'topics include', 'covers',
            'introduction to', 'deep dive', 'overview of'
        ]
        
        valid_descriptions = []
        
        # Get all paragraph texts with filtering
        for p in soup.find_all('p'):
            text = p.get_text(strip=True)
            
            # Skip very short paragraphs
            if not text or len(text) < 50:
                continue
            
            # Convert to lowercase for keyword checking
            text_lower = text.lower()
            
            # Check if this looks like a bio
            is_bio = False
            
            # Strong indicator: starts with a name pattern
            first_sentence = text.split('.')[0] if '.' in text else text
            if any(keyword in first_sentence.lower() for keyword in ['is the', 'is a', 'has been', 'was previously']):
                is_bio = True
            
            # Check for multiple bio keywords
            bio_score = sum(1 for keyword in bio_keywords if keyword in text_lower)
            if bio_score >= 2:  # If 2 or more bio keywords, likely a bio
                is_bio = True
            
            # Skip if identified as bio
            if is_bio:
                continue
            
            # Prioritize paragraphs with talk keywords
            talk_score = sum(1 for keyword in talk_keywords if keyword in text_lower)
            
            valid_descriptions.append({
                'text': text,
                'talk_score': talk_score,
                'length': len(text)
            })
        
        # Sort by talk_score (descending) then by length
        if valid_descriptions:
            valid_descriptions.sort(key=lambda x: (x['talk_score'], x['length']), reverse=True)
            
            # Return the best match
            best_description = valid_descriptions[0]['text']
            
            # If best description has no talk keywords, check if it's actually about the talk
            if valid_descriptions[0]['talk_score'] == 0:
                # Look for the paragraph that discusses the actual content
                for desc in valid_descriptions:
                    if 'hype' in desc['text'].lower() or 'ai' in desc['text'].lower() or 'technology' in desc['text'].lower():
                        return desc['text']
            
            return best_description
        
        # Fallback to meta description
        meta_desc = soup.find('meta', {'property': 'og:description'})
        if meta_desc and meta_desc.get('content'):
            content = meta_desc.get('content').strip()
            # Check if meta description is not a bio
            if not any(keyword in content.lower() for keyword in bio_keywords[:10]):
                return content
        
        return "No description available"
    
    def _extract_room(self, text: str) -> str:
        """Extract room information from page text"""
        room_match = re.search(r'\bRoom\s+[A-Za-z0-9]+\b', text, re.IGNORECASE)
        if room_match:
            return room_match.group(0)
        
        # Alternative patterns
        hall_match = re.search(r'\bHall\s+[A-Za-z0-9]+\b', text, re.IGNORECASE)
        if hall_match:
            return hall_match.group(0)
        
        return "Unknown Room"
    
    def _extract_time_window(self, text: str) -> str:
        """Extract time window (e.g., 09:00 - 10:00)"""
        time_match = re.search(r'\b\d{1,2}:\d{2}\s*[-–]\s*\d{1,2}:\d{2}\b', text)
        if time_match:
            return time_match.group(0).replace('–', '-')
        return ""
    
    def _extract_timezone(self, text: str) -> str:
        """Extract timezone information"""
        tz_match = re.search(r'\(UTC[^\)]+\)', text)
        if tz_match:
            return tz_match.group(0)
        return ""
    
    def _extract_type_and_duration(self, text: str) -> tuple:
        """Extract talk type and duration"""
        # Look for patterns like "Talk (60 min)" or "Workshop (120 min)"
        type_match = re.search(r'\b(Talk|Workshop|Keynote|Panel|Lightning Talk)\s*\((\d+)\s*min\)', text, re.IGNORECASE)
        if type_match:
            return type_match.group(1).title(), f"{type_match.group(2)} min"
        
        # Just type without duration
        type_only = re.search(r'\b(Talk|Workshop|Keynote|Panel|Lightning Talk)\b', text, re.IGNORECASE)
        if type_only:
            return type_only.group(1).title(), ""
        
        return "Talk", ""
    
    def _extract_weekday(self, text: str) -> str:
        """Extract weekday from page text"""
        days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        for day in days:
            if re.search(r'\b' + day + r'\b', text, re.IGNORECASE):
                return day
        return ""
    
    def _empty_talk_data(self, url: str) -> Dict[str, Any]:
        """Return empty talk data structure for failed parsing"""
        return {
            'title': 'Failed to Parse',
            'description': 'Error occurred while parsing this talk',
            'speaker': 'Unknown',
            'category': 'Error',
            'time_slot': 'Unknown',
            'room': 'Unknown',
            'url': url,
            'platform': 'ndc',
            'crawled_at': datetime.utcnow().isoformat()
        }


class GenericAdapter(BaseConferenceAdapter):
    """Generic adapter for unknown platforms using heuristic parsing"""
    
    async def extract_talk_urls(self) -> List[str]:
        """Heuristic URL extraction for unknown platforms"""
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.base_url, timeout=15) as response:
                    if response.status != 200:
                        return []
                    
                    html = await response.text()
                    return self._heuristic_link_extraction(html)
                    
        except Exception as e:
            print(f"Error extracting URLs from generic platform: {str(e)}")
            return []
    
    def _heuristic_link_extraction(self, html: str) -> List[str]:
        """Extract potential talk links using heuristics"""
        
        soup = BeautifulSoup(html, 'html.parser')
        potential_urls = set()
        
        # Special handling for NDC domains to avoid wrong links
        parsed = urlparse(self.base_url)
        if parsed.netloc.endswith('ndccopenhagen.com'):
            # Use NDC-specific link pattern
            for a in soup.find_all('a', href=True):
                href = a['href']
                if NDCAdapter.AGENDA_DETAIL_RE.search(href):
                    potential_urls.add(urljoin(self.base_url, href))
            return list(potential_urls)
        
        # Keywords that suggest talk/session content
        talk_keywords = [
            'session', 'talk', 'presentation', 'speaker', 'agenda',
            'schedule', 'program', 'workshop', 'keynote', 'panel'
        ]
        
        for link in soup.find_all('a', href=True):
            href = link['href']
            link_text = link.get_text().lower().strip()
            
            # Skip obviously non-talk links
            if any(skip in href.lower() for skip in ['mailto:', 'tel:', 'javascript:', '#']):
                continue
            
            # Check if URL or text suggests it's a talk
            is_talk_link = (
                any(keyword in href.lower() for keyword in talk_keywords) or
                any(keyword in link_text for keyword in talk_keywords) or
                (len(link_text) > 20 and len(link_text) < 200)  # Reasonable title length
            )
            
            if is_talk_link:
                if not href.startswith('http'):
                    href = urljoin(self.base_url, href)
                potential_urls.add(href)
        
        return list(potential_urls)
    
    async def parse_talk_page(self, talk_url: str) -> Dict[str, Any]:
        """Generic talk page parsing"""
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(talk_url, timeout=10) as response:
                    if response.status != 200:
                        return self._empty_talk_data(talk_url)
                    
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    return {
                        'title': self._extract_generic_title(soup),
                        'description': self._extract_generic_description(soup),
                        'speaker': self._extract_generic_speakers(soup),
                        'category': self._extract_generic_category(soup),
                        'time_slot': self._extract_generic_time(soup),
                        'room': self._extract_generic_room(soup),
                        'url': talk_url,
                        'platform': 'generic',
                        'crawled_at': datetime.utcnow().isoformat()
                    }
                    
        except Exception as e:
            print(f"Error parsing generic talk {talk_url}: {str(e)}")
            return self._empty_talk_data(talk_url)
    
    def _extract_generic_title(self, soup: BeautifulSoup) -> str:
        # Try multiple title extraction strategies
        selectors = [
            'h1', 'h2', '.title', '.session-title', '.talk-title',
            '.event-title', '.presentation-title'
        ]
        
        for selector in selectors:
            elem = soup.select_one(selector)
            if elem and elem.get_text().strip():
                title = elem.get_text().strip()
                if 5 < len(title) < 200:  # Reasonable title length
                    return title
        
        # Fallback to page title
        title_elem = soup.find('title')
        if title_elem:
            return title_elem.get_text().strip()
        
        return "Unknown Title"
    
    def _extract_generic_description(self, soup: BeautifulSoup) -> str:
        selectors = [
            '.description', '.abstract', '.summary', '.content',
            '.talk-description', '.session-description'
        ]
        
        for selector in selectors:
            elem = soup.select_one(selector)
            if elem and len(elem.get_text().strip()) > 50:
                return elem.get_text().strip()
        
        # Fallback: look for longest paragraph
        paragraphs = soup.find_all('p')
        longest_p = max(paragraphs, key=lambda p: len(p.get_text()), default=None)
        if longest_p and len(longest_p.get_text().strip()) > 50:
            return longest_p.get_text().strip()
        
        return "No description available"
    
    def _extract_generic_speakers(self, soup: BeautifulSoup) -> str:
        speakers = []
        selectors = [
            '.speaker', '.presenter', '.author', '.speaker-name'
        ]
        
        for selector in selectors:
            elems = soup.select(selector)
            for elem in elems:
                speaker = elem.get_text().strip()
                if speaker and len(speaker) < 100 and speaker not in speakers:
                    speakers.append(speaker)
        
        return ' & '.join(speakers) if speakers else 'Unknown Speaker'
    
    def _extract_generic_category(self, soup: BeautifulSoup) -> str:
        selectors = ['.category', '.track', '.tag', '.topic']
        for selector in selectors:
            elem = soup.select_one(selector)
            if elem and elem.get_text().strip():
                return elem.get_text().strip()
        return "Uncategorized"
    
    def _extract_generic_time(self, soup: BeautifulSoup) -> str:
        selectors = ['.time', '.schedule', '.when', '.datetime']
        for selector in selectors:
            elem = soup.select_one(selector)
            if elem and elem.get_text().strip():
                return elem.get_text().strip()
        return "Unknown Time"
    
    def _extract_generic_room(self, soup: BeautifulSoup) -> str:
        selectors = ['.room', '.location', '.venue', '.where']
        for selector in selectors:
            elem = soup.select_one(selector)
            if elem and elem.get_text().strip():
                return elem.get_text().strip()
        return "Unknown Room"
    
    def _empty_talk_data(self, url: str) -> Dict[str, Any]:
        return {
            'title': 'Failed to Parse',
            'description': 'Error occurred while parsing this talk',
            'speaker': 'Unknown',
            'category': 'Error',
            'time_slot': 'Unknown',
            'room': 'Unknown',
            'url': url,
            'platform': 'generic',
            'crawled_at': datetime.utcnow().isoformat()
        }

def get_platform_adapter(platform: str, base_url: str) -> BaseConferenceAdapter:
    """Factory function to get the appropriate adapter"""
    
    adapters = {
        'sched': SchedAdapter,
        'sessionize': SessionizeAdapter,
        'ndc': NDCAdapter,
        'generic': GenericAdapter
    }
    
    adapter_class = adapters.get(platform, GenericAdapter)
    return adapter_class(base_url)
