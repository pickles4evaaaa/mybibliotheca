"""
AI Service for book extraction from images
Handles communication with AI providers (OpenAI, Ollama) for book information extraction
"""

import base64
import json
import requests
from typing import Dict, Any, Optional
from flask import current_app
import os


class AIService:
    """Service for AI-powered book information extraction"""
    
    def __init__(self, config: Dict[str, str]):
        self.config = config
        self.provider = config.get('AI_PROVIDER', 'openai')
        self.timeout = int(config.get('AI_TIMEOUT', '30'))
        # Normalize and clamp max tokens (100..128000), warn >16k
        try:
            requested_max = int(config.get('AI_MAX_TOKENS', '1000'))
        except ValueError:
            requested_max = 1000
        self.max_tokens = min(max(requested_max, 100), 128000)
        if self.max_tokens > 16000:
            try:
                current_app.logger.warning(
                    f"AI_MAX_TOKENS set to {self.max_tokens}. Values above 16k may be slow or unsupported by some providers/models."
                )
            except Exception:
                pass
        
    def extract_book_info_from_image(self, image_data: bytes, filename: str) -> Optional[Dict[str, Any]]:
        """
        Extract book information from image using AI
        
        Args:
            image_data: Raw image file data
            filename: Name of the image file
            
        Returns:
            Dictionary with extracted book information or None if extraction failed
        """
        try:
            # Load the prompt template
            prompt = self._load_prompt_template()

            # Determine provider order with optional fallback
            primary = (self.provider or 'openai').lower()
            secondary = 'ollama' if primary == 'openai' else 'openai'
            providers_to_try = [primary]

            # Enable fallback if explicitly configured or if the alternate provider appears configured
            fallback_enabled = self.config.get('AI_FALLBACK_ENABLED', 'true').lower() == 'true'
            other_configured = (
                (secondary == 'openai' and bool(self.config.get('OPENAI_API_KEY')))
                or (secondary == 'ollama' and bool(self.config.get('OLLAMA_BASE_URL', 'http://localhost:11434')))
            )
            if fallback_enabled and other_configured:
                providers_to_try.append(secondary)

            last_error = None
            for prov in providers_to_try:
                try:
                    current_app.logger.info(f"AI extraction attempting provider: {prov}")
                    if prov == 'openai':
                        result = self._extract_with_openai(image_data, prompt)
                    elif prov == 'ollama':
                        result = self._extract_with_ollama(image_data, prompt)
                    else:
                        current_app.logger.error(f"Unknown AI provider: {prov}")
                        continue
                    if result:
                        if prov != primary:
                            current_app.logger.info(f"AI extraction succeeded with fallback provider: {prov}")
                        return result
                except Exception as e:
                    last_error = e
                    current_app.logger.warning(f"Provider {prov} failed: {e}")

            if last_error:
                current_app.logger.error(f"AI extraction failed after trying providers {providers_to_try}: {last_error}")
            return None
                
        except Exception as e:
            current_app.logger.error(f"Error in AI book extraction: {e}")
            return None
    
    def _load_prompt_template(self) -> str:
        """Load the mustache prompt template"""
        try:
            prompt_path = os.path.join(os.path.dirname(current_app.root_path), 'prompts', 'book_extraction.mustache')
            
            if os.path.exists(prompt_path):
                with open(prompt_path, 'r', encoding='utf-8') as f:
                    template_content = f.read()
                
                # Try to use pystache for templating
                try:
                    import pystache
                    # For now, we don't have template variables, so just return the content
                    # In the future, you could pass context variables here
                    return pystache.render(template_content, {})
                except ImportError:
                    # If pystache is not available, return template as-is
                    current_app.logger.warning("pystache not available, using template without processing")
                    return template_content
            else:
                # Fallback prompt if file doesn't exist
                return self._get_fallback_prompt()
        except Exception as e:
            current_app.logger.warning(f"Could not load prompt template: {e}")
            return self._get_fallback_prompt()
    
    def _get_fallback_prompt(self) -> str:
        """Fallback prompt if template file is not available"""
        return """Analyze this book cover image and extract the following information in JSON format:
        
        {
            "title": "Book title",
            "subtitle": "Subtitle if present",
            "authors": "Author names separated by semicolons",
            "isbn": "ISBN if visible",
            "publisher": "Publisher name",
            "publication_year": "Year of publication",
            "genre": "Genre or category",
            "series": "Series name if applicable",
            "contributors": [
                {"name": "Contributor Name", "role": "Editor/Translator/etc"}
            ]
        }
        
        Only include fields where you can clearly see the information. Use null for missing information. Contributors should be an array of objects with name and role."""
    
    def _extract_with_openai(self, image_data: bytes, prompt: str) -> Optional[Dict[str, Any]]:
        """Extract book info using OpenAI Vision API"""
        try:
            # Encode image to base64
            image_base64 = base64.b64encode(image_data).decode('utf-8')
            
            # Prepare API request
            headers = {
                'Authorization': f"Bearer {self.config.get('OPENAI_API_KEY')}",
                'Content-Type': 'application/json'
            }
            
            payload = {
                'model': self.config.get('OPENAI_MODEL', 'gpt-4o-mini'),
                'messages': [
                    {
                        'role': 'user',
                        'content': [
                            {
                                'type': 'text',
                                'text': prompt
                            },
                            {
                                'type': 'image_url',
                                'image_url': {
                                    'url': f"data:image/jpeg;base64,{image_base64}"
                                }
                            }
                        ]
                    }
                ],
                'max_tokens': self.max_tokens,
                'temperature': float(self.config.get('AI_TEMPERATURE', '0.1'))
            }
            
            base_url = self.config.get('OPENAI_BASE_URL', 'https://api.openai.com/v1')
            url = f"{base_url}/chat/completions"
            
            # Log the API call details
            current_app.logger.info(f"OpenAI API Request URL: {url}")
            current_app.logger.info(f"OpenAI API Request Headers: {headers}")
            current_app.logger.info(f"OpenAI API Request Payload: {json.dumps(payload, indent=2)}")
            
            response = requests.post(url, headers=headers, json=payload, timeout=self.timeout)
            
            # Log the raw response
            current_app.logger.info(f"OpenAI API Response Status: {response.status_code}")
            current_app.logger.info(f"OpenAI API Response Headers: {dict(response.headers)}")
            current_app.logger.info(f"OpenAI API Raw Response: {response.text}")
            
            response.raise_for_status()
            
            result = response.json()
            
            if 'choices' in result and len(result['choices']) > 0:
                content = result['choices'][0]['message']['content']
                current_app.logger.info(f"OpenAI extracted content: {content}")
                
                # Try to extract JSON from the response
                return self._parse_ai_response(content)
            
            return None
            
        except Exception as e:
            current_app.logger.error(f"OpenAI API error: {e}")
            current_app.logger.error(f"OpenAI API error traceback: ", exc_info=True)
            return None
    
    def _extract_with_ollama(self, image_data: bytes, prompt: str) -> Optional[Dict[str, Any]]:
        """Extract book info using Ollama local API"""
        try:
            # Encode image to base64
            image_base64 = base64.b64encode(image_data).decode('utf-8')
            
            # Prepare API request for Ollama
            headers = {
                'Content-Type': 'application/json'
            }
            
            payload = {
                'model': self.config.get('OLLAMA_MODEL', 'llama3.2-vision:11b'),
                'messages': [
                    {
                        'role': 'user',
                        'content': prompt,
                        'images': [image_base64]
                    }
                ],
                'stream': False,
                'options': {
                    'temperature': float(self.config.get('AI_TEMPERATURE', '0.1')),
                    'num_predict': self.max_tokens
                }
            }
            
            base_url = self.config.get('OLLAMA_BASE_URL', 'http://localhost:11434')
            
            # Remove /v1 suffix if present for native API  
            if base_url.endswith('/v1'):
                base_url = base_url[:-3]
            
            url = f"{base_url}/api/chat"
            
            # Log the API call details
            current_app.logger.info(f"Ollama API Request URL: {url}")
            current_app.logger.info(f"Ollama API Request Model: {payload['model']}")
            current_app.logger.info(f"Ollama API Request Options: {payload['options']}")
            current_app.logger.info(f"Ollama API Image size: {len(image_base64)} base64 characters")
            
            response = requests.post(url, headers=headers, json=payload, timeout=self.timeout)
            
            # Log the raw response
            current_app.logger.info(f"Ollama API Response Status: {response.status_code}")
            current_app.logger.info(f"Ollama API Raw Response: {response.text}")
            
            if response.status_code == 200:
                result = response.json()
                current_app.logger.info(f"Ollama parsed response: {json.dumps(result, indent=2)}")
                
                # Handle Ollama native API response format
                if 'message' in result and 'content' in result['message']:
                    content = result['message']['content']
                    current_app.logger.info(f"Ollama extracted content: {content}")
                    
                    # Try to extract JSON from the response
                    return self._parse_ai_response(content)
                else:
                    current_app.logger.error(f"Unexpected Ollama response format: {result}")
                    return None
            else:
                current_app.logger.error(f"Ollama API request failed with status {response.status_code}: {response.text}")
                return None
            
        except Exception as e:
            current_app.logger.error(f"Ollama API error: {e}")
            current_app.logger.error(f"Ollama API error traceback: ", exc_info=True)
            return None
    
    def _parse_ai_response(self, content: str) -> Optional[Dict[str, Any]]:
        """Parse AI response and extract JSON data"""
        try:
            # Try to find JSON in the response
            content = content.strip()
            
            # Look for JSON block markers
            if '```json' in content:
                start = content.find('```json') + 7
                end = content.find('```', start)
                if end > start:
                    content = content[start:end].strip()
            elif '```' in content:
                start = content.find('```') + 3
                end = content.find('```', start)
                if end > start:
                    content = content[start:end].strip()
            
            # Try to parse as JSON
            data = json.loads(content)
            
            # Clean up the data
            cleaned_data = {}
            for key, value in data.items():
                if value and value != 'null' and str(value).strip():
                    # Keep contributors as array, convert others to string
                    if key == 'contributors' and isinstance(value, list):
                        cleaned_data[key] = value
                    else:
                        cleaned_data[key] = str(value).strip()
            
            return cleaned_data if cleaned_data else None
            
        except json.JSONDecodeError as e:
            current_app.logger.warning(f"Could not parse AI response as JSON: {e}")
            current_app.logger.debug(f"Raw AI response: {content}")
            return None
        except Exception as e:
            current_app.logger.error(f"Error parsing AI response: {e}")
            return None
    
    def test_connection(self) -> Dict[str, Any]:
        """Test connection to the AI service"""
        try:
            if self.provider == 'openai':
                return self._test_openai_connection()
            elif self.provider == 'ollama':
                return self._test_ollama_connection()
            else:
                return {'success': False, 'message': f'Unknown provider: {self.provider}'}
        except Exception as e:
            return {'success': False, 'message': f'Connection test failed: {str(e)}'}
    
    def _test_openai_connection(self) -> Dict[str, Any]:
        """Test OpenAI API connection"""
        try:
            headers = {
                'Authorization': f"Bearer {self.config.get('OPENAI_API_KEY')}",
                'Content-Type': 'application/json'
            }
            
            base_url = self.config.get('OPENAI_BASE_URL', 'https://api.openai.com/v1')
            url = f"{base_url}/models"
            
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                return {'success': True, 'message': 'OpenAI connection successful'}
            elif response.status_code == 401:
                return {'success': False, 'message': 'Invalid API key'}
            else:
                return {'success': False, 'message': f'API returned status {response.status_code}'}
                
        except requests.exceptions.Timeout:
            return {'success': False, 'message': 'Connection timeout'}
        except requests.exceptions.ConnectionError:
            return {'success': False, 'message': 'Could not connect to OpenAI API'}
        except Exception as e:
            return {'success': False, 'message': f'Connection error: {str(e)}'}
    
    def _test_ollama_connection(self) -> Dict[str, Any]:
        """Test Ollama API connection and fetch available models"""
        try:
            base_url = self.config.get('OLLAMA_BASE_URL', 'http://localhost:11434')
            
            # Remove /v1 suffix if present for native API
            if base_url.endswith('/v1'):
                base_url = base_url[:-3]
            
            # Test basic connection first
            ping_url = f"{base_url}/api/tags"
            current_app.logger.info(f"Testing Ollama connection to: {ping_url}")
            
            response = requests.get(ping_url, timeout=10)
            current_app.logger.info(f"Ollama connection test response: {response.status_code} - {response.text}")
            
            if response.status_code == 200:
                result = response.json()
                
                # Extract model information
                models = []
                if 'models' in result:
                    for model in result['models']:
                        model_name = model.get('name', 'Unknown')
                        model_size = model.get('size', 0)
                        model_modified = model.get('modified_at', '')
                        
                        # Convert size to human readable
                        size_gb = model_size / (1024**3) if model_size > 0 else 0
                        size_str = f"{size_gb:.1f}GB" if size_gb > 0 else "Unknown size"
                        
                        models.append({
                            'name': model_name,
                            'size': size_str,
                            'modified': model_modified
                        })
                
                model_list = ', '.join([m['name'] for m in models]) if models else 'No models found'
                
                message = f'Ollama connection successful! Available models: {model_list}'
                if models:
                    message += f'\n\nDetailed model info:\n'
                    for model in models:
                        message += f"• {model['name']} ({model['size']})\n"
                
                return {
                    'success': True, 
                    'message': message,
                    'models': models
                }
            else:
                return {'success': False, 'message': f'Ollama server returned status {response.status_code}'}
                
        except requests.exceptions.Timeout:
            return {'success': False, 'message': 'Connection timeout - is Ollama running?'}
        except requests.exceptions.ConnectionError:
            return {'success': False, 'message': 'Could not connect to Ollama server'}
        except Exception as e:
            current_app.logger.error(f"Ollama connection test error: {e}", exc_info=True)
            return {'success': False, 'message': f'Connection error: {str(e)}'}
