/**
 * Instagram Analytics Web Application
 * 
 * This application provides a comprehensive platform for users to analyze their Instagram 
 * direct messaging data through a web-based dashboard. Users upload their Instagram 
 * data exports (ZIP files) to generate interactive visualizations and statistical reports.
 * 
 * Key Features:
 * - Chunked Upload System: Efficiently handles large ZIP files by splitting them into 
 *   manageable chunks during the upload process.
 * - Temporary Data Privacy: Implements a 3-day data retention policy with an automated 
 *   background cleanup daemon to ensure user privacy.
 * - Conversation Analytics: Provides detailed metrics for individual chats, including 
 *   total message counts, attachment ratios, and per-sender activity distributions.
 * - Timing & Behavioral Analysis: Calculates average response times, identifies the 
 *   longest/shortest gaps between messages, and detects "highest density" periods 
 *   of messaging activity.
 * - Linguistic Insights: Features frequency analysis for words and emojis, as well as 
 *   a tool to count occurrences of specific user-defined strings.
 * - Collaborative Sharing: Allows users to share specific conversation analytics with 
 *   others using unique access codes via a symlink-based sharing mechanism.
 * - Responsive UI: A modern, Tailwind CSS-powered dashboard featuring dark mode 
 *   support and interactive period-based filtering (e.g., past 7 days, custom ranges).
 * 
 * Tech Stack:
 * - Backend: Python (Flask), Werkzeug, Zipfile, Emoji.
 * - Frontend: HTML5, JavaScript (ES6+), Tailwind CSS.
 */

# Usage

only tested on Linux (Ubuntu Server 25.04 x86_64)
```
git clone https://github.com/chronosirius/chv 
cd chv
python3 -m venv venv
source venv/bin/activate
pip install python-dotenv emoji flask
python app.py
```