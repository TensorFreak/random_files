"""
Email parser for .msg and .eml files with attachment extraction.
Handles email threads and organizes attachments by message.
"""

import os
import email
from email import policy
from pathlib import Path
from typing import List, Dict, Any
import extract_msg  # pip install extract-msg


def parse_eml_file(eml_path: str) -> Dict[str, Any]:
    """Parse .eml file and extract metadata and attachments."""
    from email.header import decode_header
    
    with open(eml_path, 'rb') as f:
        msg = email.message_from_binary_file(f, policy=policy.default)
    
    def decode_mime_header(header_value):
        """Decode MIME encoded header."""
        if not header_value:
            return ''
        decoded_parts = decode_header(header_value)
        result = []
        for text, charset in decoded_parts:
            if isinstance(text, bytes):
                try:
                    result.append(text.decode(charset or 'utf-8'))
                except (UnicodeDecodeError, LookupError):
                    for enc in ['utf-8', 'cp1251', 'koi8-r', 'latin-1']:
                        try:
                            result.append(text.decode(enc))
                            break
                        except (UnicodeDecodeError, LookupError):
                            continue
                    else:
                        result.append(text.decode(errors='ignore'))
            else:
                result.append(str(text))
        return ''.join(result)
    
    body_data = _extract_body(msg)
    
    return {
        'subject': decode_mime_header(msg.get('subject', 'No Subject')),
        'from': decode_mime_header(msg.get('from', '')),
        'to': decode_mime_header(msg.get('to', '')),
        'date': msg.get('date', ''),
        'body': body_data['text'],
        'body_html': body_data['html'],
        'attachments': _extract_attachments_eml(msg)
    }


def parse_msg_file(msg_path: str) -> Dict[str, Any]:
    """Parse .msg file and extract metadata and attachments."""
    msg = extract_msg.Message(msg_path)
    
    attachments = []
    for attachment in msg.attachments:
        attachments.append({
            'filename': attachment.longFilename or attachment.shortFilename,
            'data': attachment.data
        })
    
    # Handle body_html - it can be bytes or string
    body_html = msg.htmlBody or ''
    if isinstance(body_html, bytes):
        body_html = _decode_payload(body_html)
    elif isinstance(body_html, str):
        # Sometimes extract_msg returns string but with wrong encoding
        # Try to detect if it's been incorrectly decoded
        try:
            # Check if the string contains mojibake (garbled text)
            # If it has lots of weird characters, try to re-encode and decode properly
            if any(ord(c) > 1000 for c in body_html[:100]) or 'Ð' in body_html or 'Ñ' in body_html:
                # Try to get raw bytes and re-decode
                # This is a heuristic - the string might be latin-1 decoded cp1251
                try:
                    body_html_bytes = body_html.encode('latin-1')
                    body_html = _decode_payload(body_html_bytes)
                except:
                    pass
        except:
            pass
    
    result = {
        'subject': msg.subject or 'No Subject',
        'from': msg.sender or '',
        'to': msg.to or '',
        'date': str(msg.date) if msg.date else '',
        'body': msg.body or '',
        'body_html': body_html,
        'attachments': attachments
    }
    
    msg.close()
    return result


def _decode_payload(payload: bytes, charset: str = None) -> str:
    """Decode payload with proper charset handling."""
    if not payload:
        return ""
    
    # If charset is provided, try it first
    if charset:
        try:
            return payload.decode(charset)
        except (UnicodeDecodeError, LookupError):
            pass
    
    # Try to detect encoding automatically
    try:
        import chardet
        detected = chardet.detect(payload)
        if detected and detected['encoding'] and detected['confidence'] > 0.7:
            try:
                return payload.decode(detected['encoding'])
            except (UnicodeDecodeError, LookupError):
                pass
    except ImportError:
        pass
    
    # Try common encodings in order of likelihood for email content
    # For Russian content: cp1251 (Windows Cyrillic) is most common
    for enc in ['utf-8', 'cp1251', 'windows-1251', 'koi8-r', 'iso-8859-5', 'latin-1']:
        try:
            return payload.decode(enc)
        except (UnicodeDecodeError, LookupError):
            continue
    
    # Last resort: decode with errors ignored
    return payload.decode('utf-8', errors='ignore')


def _extract_body(msg: email.message.Message) -> Dict[str, str]:
    """Extract plain text and HTML body from email message."""
    text_body = ""
    html_body = ""
    
    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            payload = part.get_payload(decode=True)
            
            if content_type == "text/plain" and not text_body and payload:
                charset = part.get_content_charset()
                text_body = _decode_payload(payload, charset)
            
            elif content_type == "text/html" and not html_body and payload:
                charset = part.get_content_charset()
                html_body = _decode_payload(payload, charset)
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            charset = msg.get_content_charset()
            content_type = msg.get_content_type()
            
            if content_type == "text/plain":
                text_body = _decode_payload(payload, charset)
            elif content_type == "text/html":
                html_body = _decode_payload(payload, charset)
    
    return {'text': text_body, 'html': html_body}


def _extract_attachments_eml(msg: email.message.Message) -> List[Dict[str, Any]]:
    """Extract attachments from .eml message."""
    attachments = []
    for part in msg.walk():
        if part.get_content_disposition() == 'attachment':
            filename = part.get_filename()
            if filename:
                # Decode filename if it's encoded
                if filename and '=?' in filename:
                    from email.header import decode_header
                    decoded_parts = decode_header(filename)
                    filename = ''.join([
                        text.decode(charset or 'utf-8') if isinstance(text, bytes) else text
                        for text, charset in decoded_parts
                    ])
                
                attachments.append({
                    'filename': filename,
                    'data': part.get_payload(decode=True)
                })
    return attachments


def parse_email_file(email_path: str) -> Dict[str, Any]:
    """Parse email file based on extension (.msg or .eml)."""
    ext = Path(email_path).suffix.lower()
    if ext == '.eml':
        return parse_eml_file(email_path)
    elif ext == '.msg':
        return parse_msg_file(email_path)
    else:
        raise ValueError(f"Unsupported email format: {ext}")


def split_email_thread(parsed_email: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Split email body into individual messages if it's a thread.
    Returns list of message dictionaries.
    """
    body = parsed_email.get('body', '')
    messages = []
    
    # Common email thread separators
    separators = [
        '\n________________________________\n',
        '\n-----Original Message-----\n',
        '\nFrom:',
        '\n\nOn ',  # "On [date], [person] wrote:"
    ]
    
    # Simple split - you can enhance this with regex
    parts = [body]
    for sep in separators:
        new_parts = []
        for part in parts:
            new_parts.extend(part.split(sep))
        parts = new_parts
    
    # Create message entries
    for idx, part in enumerate(parts):
        if part.strip():
            msg = parsed_email.copy()
            msg['body'] = part.strip()
            msg['thread_index'] = idx
            # First message keeps original attachments
            if idx == 0:
                msg['attachments'] = parsed_email['attachments']
            else:
                msg['attachments'] = []  # Sub-messages typically don't have separate attachments
            messages.append(msg)
    
    return messages if len(messages) > 1 else [parsed_email]


def save_attachments(attachments: List[Dict[str, Any]], output_dir: str):
    """Save attachments to specified directory."""
    os.makedirs(output_dir, exist_ok=True)
    
    for attachment in attachments:
        filename = attachment['filename']
        filepath = os.path.join(output_dir, filename)
        
        # Handle duplicate filenames
        counter = 1
        base, ext = os.path.splitext(filename)
        while os.path.exists(filepath):
            filename = f"{base}_{counter}{ext}"
            filepath = os.path.join(output_dir, filename)
            counter += 1
        
        with open(filepath, 'wb') as f:
            f.write(attachment['data'])


def process_email(email_path: str, output_base_dir: str = './parsed_emails'):
    """
    Main function to process email file and organize attachments.
    
    Structure:
    output_base_dir/
        email_filename/
            message_0/
                attachment1.pdf
                attachment2.xlsx
            message_1/
                attachment3.doc
    """
    # Parse email
    parsed = parse_email_file(email_path)
    
    # Create folder name from email filename (without extension)
    email_name = Path(email_path).stem
    email_dir = os.path.join(output_base_dir, email_name)
    
    # Split into thread messages
    messages = split_email_thread(parsed)
    
    # Save each message's attachments
    for idx, message in enumerate(messages):
        if message['attachments']:
            message_dir = os.path.join(email_dir, f"message_{idx}")
            save_attachments(message['attachments'], message_dir)
            print(f"Saved {len(message['attachments'])} attachment(s) to {message_dir}")
    
    print(f"Processed email: {email_name}")
    print(f"Total messages: {len(messages)}")


def process_email_folder(folder_path: str, output_base_dir: str = './parsed_emails'):
    """Process all .msg and .eml files in a folder."""
    folder = Path(folder_path)
    
    for email_file in folder.glob('*'):
        if email_file.suffix.lower() in ['.msg', '.eml']:
            try:
                print(f"\nProcessing: {email_file.name}")
                process_email(str(email_file), output_base_dir)
            except Exception as e:
                print(f"Error processing {email_file.name}: {e}")


if __name__ == "__main__":
    # Example usage
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python email_parser.py <email_file_or_folder>")
        sys.exit(1)
    
    path = sys.argv[1]
    
    if os.path.isfile(path):
        process_email(path)
    elif os.path.isdir(path):
        process_email_folder(path)
    else:
        print(f"Path not found: {path}")
