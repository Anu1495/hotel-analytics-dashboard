# Hotel Indigo London Paddington
# 771-129-1295
# Mercure Hyde Park Hotel
# 129-604-5272
# Mercure Nottingham Hotel
# 378-794-0566

import streamlit as st
# Add these imports at the top of your file
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import webbrowser
import base64
from urllib.parse import urlparse, parse_qs
import requests
# Set page config must be the first Streamlit command
st.set_page_config(page_title="Ecommerce Dashboard", layout="wide", page_icon="📊")
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from google.analytics.data_v1beta import BetaAnalyticsDataClient
import numpy as np
from google.analytics.data_v1beta.types import (
    DateRange, Dimension, Metric, RunReportRequest, 
    Filter, FilterExpression, FilterExpressionList
)
from datetime import datetime, timedelta
from google.oauth2 import service_account
from google.analytics.admin import AnalyticsAdminServiceClient
from google.analytics.admin_v1alpha.types import (
    Audience, AudienceFilterClause, 
    AudienceFilterExpression, AudienceSimpleFilter,
    AudienceFilterScope
)
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
import sys
from pathlib import Path
import logging
from logging.handlers import RotatingFileHandler
import traceback
import os
try:
    import matplotlib
except ImportError:
    pass  # We'll handle this gracefully in the functions that need it
import calendar
from dateutil.relativedelta import relativedelta
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from dotenv import load_dotenv
import codecs
import json
from pathlib import Path
from google.auth.exceptions import RefreshError

# Custom CSS for styling
st.markdown("""
<style>
    /* Main container styling */
    .main {
        background-color: #f8f9fa;
    }
    
    /* KPI styling */
    .kpi-card {
        padding: 15px;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        background-color: white;
        border-left: 5px solid #4285F4;
    }
    
    /* Title styling */
    .dashboard-title {
        color: #2c3e50;
        font-size: 2.5rem;
        font-weight: bold;
        margin-bottom: 20px;
    }
    
    /* Subheader styling */
    .subheader {
        color: #2c3e50;
        font-size: 1.5rem;
        font-weight: bold;
        margin-top: 20px;
        margin-bottom: 15px;
    }
    
    /* Tab styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 10px;
    }
    
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        padding: 0 25px;
        background-color: #f8f9fa;
        border-radius: 10px 10px 0 0;
        border: 1px solid #e0e0e0;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    
    .stTabs [aria-selected="true"] {
        background-color: white;
        border-bottom: 3px solid #4285F4;
    }
    
    /* Button styling */
    .stButton>button {
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        border: 1px solid #e0e0e0;
    }
    
    /* Selectbox styling */
    .stSelectbox>div>div {
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    
    /* Date input styling */
    .stDateInput>div>div>input {
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    
    /* Radio button styling */
    .stRadio>div {
        gap: 15px;
    }
    
    .stRadio [role="radiogroup"] {
        background-color: white;
        padding: 15px;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    
    /* Dataframe styling */
    .stDataFrame {
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    
    /* Expander styling */
    .stExpander {
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
        border: 1px solid #e0e0e0;
    }
    
    .stExpander>summary {
        background-color: white;
        border-radius: 10px 10px 0 0;
        padding: 15px;
        font-weight: bold;
    }
    
    .stExpander>div {
        padding: 15px;
        border-radius: 0 0 10px 10px;
    }
</style>
""", unsafe_allow_html=True)

def calculate_roi_by_hotel(ga_properties, ads_accounts, start_date, end_date):
    """Calculate ROI for each hotel by combining GA revenue with Ads cost"""
    roi_data = []
    
    for property_id, hotel_name in ga_properties.items():
        # Get corresponding Google Ads account ID
        ads_account_id = None
        for acc_id, acc_name in ads_accounts.items():
            if hotel_name.lower() in acc_name.lower():
                ads_account_id = acc_id
                break
        
        if not ads_account_id:
            st.warning(f"No matching Google Ads account found for {hotel_name}")
            continue
            
        # Fetch GA revenue data
        with st.spinner(f"Fetching GA revenue for {hotel_name}..."):
            ga_data = fetch_ga4_data(property_id, start_date, end_date)
            total_revenue = ga_data['revenue'].sum()
        
        # Fetch Ads cost data
        with st.spinner(f"Fetching Ads cost for {hotel_name}..."):
            ads_config = GoogleAdsConfig(customer_id=ads_account_id)
            ads_manager = GoogleAdsManager(ads_config)
            if ads_manager.initialize_client():
                ads_data = ads_manager.fetch_google_ads_data(
                    customer_id=ads_account_id,
                    start_date=start_date,
                    end_date=end_date
                )
                total_cost = ads_data['cost'].sum() if not ads_data.empty else 0
            else:
                total_cost = 0
        
        # Calculate ROI metrics
        roi = (total_revenue) / total_cost if total_cost > 0 else 0
        profit = total_revenue - total_cost
        
        roi_data.append({
            'Hotel': hotel_name,
            'GA Property': property_id,
            'Ads Account': ads_account_id,
            'Revenue (£)': total_revenue,
            'Ad Spend (£)': total_cost,
            'Profit (£)': profit,
            'ROI': roi
        })
    
    return pd.DataFrame(roi_data)

def safe_load_dotenv(env_path=None):
    """Safely load environment variables, with Azure compatibility"""
    try:
        # Try loading from .env file if path is provided (for local development)
        if env_path and Path(env_path).exists():
            load_dotenv(env_path, encoding='utf-8')
        
        # Verify we have required variables (works for both .env and Azure env vars)
        required_vars = [
            "GOOGLE_ADS_CLIENT_ID",
            "GOOGLE_ADS_CLIENT_SECRET", 
            "GOOGLE_ADS_DEVELOPER_TOKEN",
            "GOOGLE_ADS_REFRESH_TOKEN"
        ]
        
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            st.error(f"Missing required environment variables: {', '.join(missing_vars)}")
            return False
        return True
    except Exception as e:
        st.error(f"Failed to load environment variables: {str(e)}")
        return False


class GoogleAdsConfig:
    def __init__(self, customer_id=None, is_manager=False):
        """Initialize Google Ads configuration"""
        # Try loading without env_path first (for Azure)
        if not safe_load_dotenv():
            # Fallback to local .env if in development
            if not safe_load_dotenv(Path.home() / "googleads.env"):
                st.error("Failed to load Google Ads configuration")
                st.stop()
        
        self.client_id = os.getenv("GOOGLE_ADS_CLIENT_ID")
        self.client_secret = os.getenv("GOOGLE_ADS_CLIENT_SECRET")
        self.developer_token = os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN")
        self.refresh_token = os.getenv("GOOGLE_ADS_REFRESH_TOKEN")
        
        # MCC ID (Manager account)
        self.login_customer_id = os.getenv("GOOGLE_ADS_LOGIN_CUSTOMER_ID") if is_manager else None
        
        # The customer ID we want to access
        self.customer_id = str(customer_id) if customer_id else None
        
        # Token storage file
        self.token_file = Path.home() / ".google_ads_token.json"
        self.access_token = None
        self.token_expiry = None
        
        # Load existing tokens if available
        self._load_tokens()
        
        # Validate required credentials
        if not all([self.client_id, self.client_secret, self.developer_token]):
            st.error("Missing required Google Ads credentials in .env file")
            st.stop()

    def _load_tokens(self):
        """Load tokens from file if they exist"""
        try:
            if self.token_file.exists():
                with open(self.token_file, 'r') as f:
                    token_data = json.load(f)
                    self.access_token = token_data.get('access_token')
                    expiry = token_data.get('token_expiry')
                    if expiry:
                        self.token_expiry = datetime.fromisoformat(expiry)
        except Exception as e:
            st.warning(f"Failed to load tokens from file: {str(e)}")
            # In Azure, you might want to use Azure Blob Storage instead:
            # self._load_tokens_from_azure_blob()

    def _save_tokens(self):
        """Save tokens to file"""
        try:
            token_data = {
                'access_token': self.access_token,
                'token_expiry': self.token_expiry.isoformat() if self.token_expiry else None
            }
            with open(self.token_file, 'w') as f:
                json.dump(token_data, f)
        except Exception as e:
            st.warning(f"Failed to save tokens to file: {str(e)}")

    def get_credentials(self):
        """Get or refresh access token with enhanced error handling"""
        try:
            # First try to use existing token if not expired
            if (self.access_token and self.token_expiry and 
                datetime.now() < self.token_expiry - timedelta(minutes=5)):
                return {
                    "access_token": self.access_token,
                    "refresh_token": self.refresh_token,
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "token_uri": "https://oauth2.googleapis.com/token"
                }
                
            # If no refresh token, guide user to generate one
            if not self.refresh_token:
                st.error("No refresh token available. Please generate new tokens.")
                display_token_generation_ui()
                return None
                
            # Refresh token if expired or about to expire
            token_url = "https://oauth2.googleapis.com/token"
            data = {
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'refresh_token': self.refresh_token,
                'grant_type': 'refresh_token'
            }
            
            response = requests.post(token_url, data=data)
            response.raise_for_status()
            
            token_data = response.json()
            self.access_token = token_data['access_token']
            expires_in = token_data.get('expires_in', 3600)
            self.token_expiry = datetime.now() + timedelta(seconds=expires_in)
            
            # Save the new tokens
            self._save_tokens()
            
            return {
                "access_token": self.access_token,
                "refresh_token": self.refresh_token,
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "token_uri": "https://oauth2.googleapis.com/token"
            }
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 400:
                st.error("Refresh token expired or revoked. Please generate new tokens.")
                display_token_generation_ui()
            else:
                st.error(f"Failed to refresh token: {str(e)}")
            return None
        except Exception as e:
            st.error(f"Authentication error: {str(e)}")
            st.error(traceback.format_exc())
            return None

class OAuthTokenGenerator:
    def __init__(self):
        self.client_id = os.getenv("GOOGLE_ADS_CLIENT_ID")
        self.client_secret = os.getenv("GOOGLE_ADS_CLIENT_SECRET")
        self.redirect_uri = "http://localhost:8080"  # Must match your Google Cloud Console settings
        
    def get_auth_url(self):
        """Generate the authorization URL"""
        flow = InstalledAppFlow.from_client_config(
            {
                "installed": {
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://oauth2.googleapis.com/token",
                    "redirect_uris": [self.redirect_uri]
                }
            },
            scopes=["https://www.googleapis.com/auth/adwords"],
            redirect_uri=self.redirect_uri
        )
        
        auth_url, _ = flow.authorization_url(
            access_type='offline',
            prompt='consent',
            include_granted_scopes='true'
        )
        return auth_url
    
    def exchange_code_for_token(self, code):
        """Exchange authorization code for tokens"""
        flow = InstalledAppFlow.from_client_config(
            {
                "installed": {
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                    "token_uri": "https://oauth2.googleapis.com/token",
                    "redirect_uris": [self.redirect_uri]
                }
            },
            scopes=["https://www.googleapis.com/auth/adwords"],
            redirect_uri=self.redirect_uri
        )
        
        flow.fetch_token(code=code)
        return flow.credentials

def display_token_generation_ui():
    """Display UI for generating new OAuth tokens"""
    with st.expander("🔑 Generate New OAuth Tokens (Click when tokens expire)"):
        st.warning("Follow these steps when your tokens expire or get revoked:")
        
        # Step 1: Get authorization URL
        if st.button("1. Get Authorization URL"):
            try:
                token_gen = OAuthTokenGenerator()
                auth_url = token_gen.get_auth_url()
                
                # Store in session state
                st.session_state.auth_url = auth_url
                
                # Try to open browser automatically
                try:
                    webbrowser.open_new_tab(auth_url)
                    st.success("Opened browser for authorization. If it didn't work, copy this URL:")
                except:
                    st.info("Please manually copy this authorization URL and open it in your browser:")
                
                st.code(auth_url)
                
            except Exception as e:
                st.error(f"Failed to generate authorization URL: {str(e)}")
        
        # Step 2: Handle the callback (manual for Streamlit)
        if 'auth_url' in st.session_state:
            st.markdown("### 2. Paste the redirect URL here after authorization")
            redirect_url = st.text_input("Paste the URL you were redirected to after authorizing:")
            
            if redirect_url:
                try:
                    # Parse the code from the redirect URL
                    parsed = urlparse(redirect_url)
                    code = parse_qs(parsed.query).get('code', [None])[0]
                    
                    if not code:
                        st.error("No authorization code found in the URL")
                        return
                    
                    # Exchange code for tokens
                    token_gen = OAuthTokenGenerator()
                    credentials = token_gen.exchange_code_for_token(code)
                    
                    # Display the tokens
                    st.success("Tokens generated successfully!")
                    st.json({
                        "access_token": credentials.token,
                        "refresh_token": credentials.refresh_token,
                        "token_uri": credentials.token_uri,
                        "expires_at": credentials.expiry.isoformat() if credentials.expiry else None
                    })
                    
                    # Provide instructions for updating .env file
                    st.markdown("### 3. Update your `.env` file with these values:")
                    st.code(f"""
GOOGLE_ADS_ACCESS_TOKEN={credentials.token}
GOOGLE_ADS_REFRESH_TOKEN={credentials.refresh_token}
GOOGLE_ADS_TOKEN_EXPIRY={credentials.expiry.timestamp() if credentials.expiry else 3600}
                    """)
                    
                    # Offer to save to file
                    if st.button("Save to .env file"):
                        env_path = r"C:\Users\anupa\googleads.env"
                        try:
                            with open(env_path, 'a') as f:
                                f.write(f"\n# Updated at {datetime.now().isoformat()}\n")
                                f.write(f"GOOGLE_ADS_ACCESS_TOKEN={credentials.token}\n")
                                f.write(f"GOOGLE_ADS_REFRESH_TOKEN={credentials.refresh_token}\n")
                                f.write(f"GOOGLE_ADS_TOKEN_EXPIRY={credentials.expiry.timestamp() if credentials.expiry else 3600}\n")
                            st.success(f"Tokens saved to {env_path}")
                        except Exception as e:
                            st.error(f"Failed to save tokens: {str(e)}")
                
                except Exception as e:
                    st.error(f"Failed to exchange authorization code: {str(e)}")
                    st.error(traceback.format_exc())
def display_roi_metrics_card(property_id, property_name, ads_account_id, start_date, end_date):
    """Display ROI metrics card for the selected hotel"""
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.subheader(f"📊 ROI Metrics for {property_name}")
    
    col1, col2 = st.columns(2)
    with col1:
        st.markdown(f"**GA4 Property:** `{property_id}`")
    with col2:
        st.markdown(f"**Google Ads Account:** `{ads_account_id}`")
    
    # Fetch GA revenue from paid sources only
    with st.spinner("Fetching GA revenue from paid sources..."):
        ga_revenue = fetch_ga4_paid_revenue(property_id, start_date, end_date)
        total_revenue = ga_revenue['revenue'].sum() if not ga_revenue.empty else 0
    
    # Fetch Google Ads spend
    with st.spinner("Fetching Google Ads spend..."):
        ads_config = GoogleAdsConfig(customer_id=ads_account_id)
        ads_manager = GoogleAdsManager(ads_config)
        if ads_manager.initialize_client():
            ads_data = ads_manager.fetch_google_ads_data(
                customer_id=ads_account_id,
                start_date=start_date,
                end_date=end_date
            )
            total_spend = ads_data['cost'].sum() if not ads_data.empty else 0
        else:
            total_spend = 0
    
    # Calculate ROI metrics
    profit = total_revenue - total_spend
    roi = (total_revenue / total_spend) if total_spend > 0 else 0
    
    # Display metrics
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric(
            "Revenue from Paid Sources",
            f"£{total_revenue:,.2f}",
            help="Total revenue from Google Ads traffic (Cross Network and Paid Search)"
        )
    
    with col2:
        st.metric(
            "Google Ads Spend",
            f"£{total_spend:,.2f}",
            help="Total cost from Google Ads campaigns"
        )
    
    with col3:
        st.metric(
            "ROI",
            f"{roi:,.2f}",
            f"£{profit:,.2f} Profit",
            help="Return on Investment: (Revenue - Spend) / Spend"
        )
    
    # Add date range info
    st.caption(f"Date range: {start_date} to {end_date}")
    st.markdown('</div>', unsafe_allow_html=True)
def fetch_ga4_paid_revenue(property_id, start_date, end_date):
    """Fetch GA4 revenue data from paid sources only (Cross Network and Paid Search)"""
    try:
        client = get_ga_client()
        
        # Filter for paid traffic sources
        request = RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=[Dimension(name="date"), Dimension(name="sessionSourceMedium")],
            metrics=[Metric(name="purchaseRevenue")],
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            dimension_filter=FilterExpression(
                or_group=FilterExpressionList(
                    expressions=[
                        FilterExpression(filter=Filter(
                            field_name="sessionSourceMedium",
                            string_filter=Filter.StringFilter(value="google / cpc"))),
                        FilterExpression(filter=Filter(
                            field_name="sessionSourceMedium",
                            string_filter=Filter.StringFilter(value="google / paidsearch")))
                    ]
                )
            ))
        
        response = client.run_report(request)
        
        data = []
        for row in response.rows:
            date = row.dimension_values[0].value
            source_medium = row.dimension_values[1].value
            revenue = float(row.metric_values[0].value)
            
            data.append({
                'date': date,
                'source_medium': source_medium,
                'revenue': revenue
            })
        
        # Return DataFrame with revenue column, even if empty
        df = pd.DataFrame(data)
        if df.empty:
            return pd.DataFrame(columns=['date', 'source_medium', 'revenue'])
        return df
        
    except Exception as e:
        st.error(f"Failed to fetch GA4 paid revenue data: {str(e)}")
        return pd.DataFrame(columns=['date', 'source_medium', 'revenue'])
class GoogleAdsManager:
    def __init__(self, config):
        self.config = config
        self.client = None
        
    def initialize_client(self):
        """Initialize Google Ads client with proper headers"""
        try:
            credentials = self.config.get_credentials()
            if not credentials:
                st.error("Failed to get credentials")
                return False
                
            # Configuration dictionary
            googleads_config = {
                "developer_token": self.config.developer_token,
                "client_id": self.config.client_id,
                "client_secret": self.config.client_secret,
                "refresh_token": self.config.refresh_token,
                "use_proto_plus": True,
                "access_token": credentials['access_token']
            }
            
            # Only add login_customer_id if accessing a client account through a manager
            if self.config.login_customer_id and self.config.customer_id != self.config.login_customer_id:
                googleads_config["login_customer_id"] = str(self.config.login_customer_id)
            
            self.client = GoogleAdsClient.load_from_dict(googleads_config)
            return True
            
        except Exception as e:
            st.error(f"Client initialization failed: {str(e)}")
            st.error(traceback.format_exc())
            return False
    
    def get_ad_groups(self, customer_id, campaign_id):
        """Fetch ad groups for a specific campaign"""
        try:
            if not self.client:
                if not self.initialize_client():
                    st.error("Failed to initialize Google Ads client")
                    return pd.DataFrame(columns=['id', 'name', 'status'])
                
            ga_service = self.client.get_service("GoogleAdsService")
            
            query = f"""
                SELECT
                    ad_group.id,
                    ad_group.name,
                    ad_group.status
                FROM ad_group
                WHERE campaign.id = {campaign_id}
                AND campaign.status = ENABLED
                AND ad_group.status = ENABLED
                ORDER BY ad_group.name
            """
        
        # Rest of the method remains the same...
            
            search_request = self.client.get_type("SearchGoogleAdsRequest")
            search_request.customer_id = customer_id
            search_request.query = query
            
            response = ga_service.search(request=search_request)
            
            ad_groups = []
            for row in response:
                ad_groups.append({
                    'id': str(row.ad_group.id),
                    'name': row.ad_group.name,
                    'status': str(row.ad_group.status)
                })
            
            return pd.DataFrame(ad_groups)
        except Exception as e:
            st.error(f"Failed to fetch ad groups: {str(e)}")
            return pd.DataFrame(columns=['id', 'name', 'status'])
        # Add this to your GoogleAdsManager class
    def clone_campaign_for_new_date(self, customer_id, base_campaign_id, new_date):
        """Public method to clone a campaign for a new date"""
        if not self.client:
            if not self.initialize_client():
                st.error("Failed to initialize Google Ads client")
                return None
        
        return create_similar_campaign(self.client, customer_id, base_campaign_id, new_date)
    def create_responsive_search_ad(self, customer_id, ad_group_id, headlines, descriptions, final_urls, path1=None, path2=None):
        """Create a responsive search ad"""
        try:
            client = self.client
            ad_group_ad_service = client.get_service("AdGroupAdService")
            ad_group_service = client.get_service("AdGroupService")
            
            # Create the ad group ad
            ad_group_ad_operation = client.get_type("AdGroupAdOperation")
            ad_group_ad = ad_group_ad_operation.create
            
            # Set the ad group
            ad_group_ad.ad_group = ad_group_service.ad_group_path(
                customer_id, ad_group_id
            )
            
            # Set the ad type to responsive search ad
            ad_group_ad.ad.final_urls.extend([final_urls])
            
            # Set the ad rotation mode
            ad_group_ad.ad.ad_rotation_mode = client.enums.AdRotationModeEnum.OPTIMIZE
            
            # Set the responsive search ad info
            responsive_search_ad_info = ad_group_ad.ad.responsive_search_ad
            
            # Add headlines
            for i, headline in enumerate(headlines[:15]):  # Max 15 headlines
                ad_text_asset = client.get_type("AdTextAsset")
                ad_text_asset.text = headline
                if i == 0:
                    ad_text_asset.pinned_field = client.enums.ServedAssetFieldTypeEnum.HEADLINE_1
                elif i == 1:
                    ad_text_asset.pinned_field = client.enums.ServedAssetFieldTypeEnum.HEADLINE_2
                responsive_search_ad_info.headlines.append(ad_text_asset)
            
            # Add descriptions
            for i, description in enumerate(descriptions[:4]):  # Max 4 descriptions
                ad_text_asset = client.get_type("AdTextAsset")
                ad_text_asset.text = description
                if i == 0:
                    ad_text_asset.pinned_field = client.enums.ServedAssetFieldTypeEnum.DESCRIPTION_1
                elif i == 1:
                    ad_text_asset.pinned_field = client.enums.ServedAssetFieldTypeEnum.DESCRIPTION_2
                responsive_search_ad_info.descriptions.append(ad_text_asset)
            
            # Set path fields if provided
            if path1:
                ad_group_ad.ad.final_url_suffix = f"lp={path1}"
            if path2:
                ad_group_ad.ad.final_url_suffix = f"{ad_group_ad.ad.final_url_suffix}&sub={path2}" if hasattr(ad_group_ad.ad, 'final_url_suffix') else f"sub={path2}"
            
            # Create the ad
            response = ad_group_ad_service.mutate_ad_group_ads(
                customer_id=customer_id,
                operations=[ad_group_ad_operation]
            )
            
            return response.results[0].resource_name
        except Exception as e:
            st.error(f"Failed to create responsive search ad: {str(e)}")
            return None
    def fetch_google_ads_data(self, customer_id, start_date, end_date, test_mode=False):
        """Fetch Google Ads data for the specified customer and date range"""
        try:
            if not self.client:
                if not self.initialize_client():
                    st.error("Google Ads client not initialized")
                    return pd.DataFrame()
    
            # Verify we have a valid access token
            if not hasattr(self.config, 'access_token') or not self.config.access_token:
                st.error("No valid access token available")
                return pd.DataFrame()
    
            client = self.client
            ga_service = client.get_service("GoogleAdsService")
            
            if test_mode:
                # Simplified test query
                query = f"""
                    SELECT
                        campaign.id,
                        campaign.name,
                        metrics.impressions
                    FROM campaign
                    WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
                    AND campaign.status = ENABLED
                    LIMIT 5
                """
            else:
                # Full query
                query = f"""
                    SELECT
                        campaign.id,
                        campaign.name,
                        metrics.impressions,
                        metrics.clicks,
                        metrics.cost_micros,
                        metrics.conversions,
                        metrics.conversions_value,
                        segments.date
                    FROM campaign
                    WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
                    AND campaign.status = ENABLED
                    ORDER BY segments.date
                """
            
            # Execute the query
            search_request = client.get_type("SearchGoogleAdsRequest")
            search_request.customer_id = customer_id
            search_request.query = query
            
            # Set login-customer-id header if this is a manager accessing a client
            if self.config.login_customer_id and customer_id != self.config.login_customer_id:
                search_request.login_customer_id = self.config.login_customer_id
            
            response = ga_service.search(request=search_request)
            
            data = []
            for row in response:
                try:
                    if test_mode:
                        data.append({
                            "campaign_id": row.campaign.id,
                            "campaign_name": row.campaign.name,
                            "impressions": row.metrics.impressions
                        })
                    else:
                        # Convert date string to datetime object
                        date_str = row.segments.date
                        date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
                        
                        data.append({
                            "date": date_obj,  # Store as date object
                            "campaign_id": row.campaign.id,
                            "campaign_name": row.campaign.name,
                            "impressions": row.metrics.impressions,
                            "clicks": row.metrics.clicks,
                            "cost": row.metrics.cost_micros / 1000000,  # Convert micros to standard currency
                            "conversions": row.metrics.conversions,
                            "conversion_value": row.metrics.conversions_value,
                            "ctr": (row.metrics.clicks / row.metrics.impressions) if row.metrics.impressions > 0 else 0,
                            "cpc": (row.metrics.cost_micros / 1000000) / row.metrics.clicks if row.metrics.clicks > 0 else 0,
                            "roas": row.metrics.conversions_value / (row.metrics.cost_micros / 1000000) if row.metrics.cost_micros > 0 else 0
                        })
                except Exception as e:
                    st.warning(f"Error processing row: {str(e)}")
                    continue
            
            if not data:
                st.warning("No data returned from API")
                return pd.DataFrame()
                
            df = pd.DataFrame(data)
            
            if not test_mode and 'date' in df.columns:
                # Convert date to datetime and sort
                df['date'] = pd.to_datetime(df['date'])
                df = df.sort_values('date')
            
            return df
        
        except GoogleAdsException as ex:
            st.error(f'Google Ads API Error: {ex.error.code().name}')
            st.error(f'Request ID: {ex.request_id}')
            
            # More detailed error information
            if ex.error.code().name == 'INVALID_ARGUMENT':
                st.error("Common causes for this error:")
                st.error("1. Invalid date format (should be YYYY-MM-DD)")
                st.error("2. Invalid customer ID format")
                st.error("3. Unsupported fields in query")
            
            for error in ex.failure.errors:
                st.error(f'- {error.message}')
                if error.location:
                    for field_path_element in error.location.field_path_elements:
                        st.error(f"\tField: {field_path_element.field_name}")
            
            return pd.DataFrame()
            
        except Exception as e:
            st.error(f"Failed to fetch Google Ads data: {str(e)}")
            st.error(traceback.format_exc())
            return pd.DataFrame()
    def get_campaigns(self, customer_id):
        """Fetch list of campaigns for dropdown selection"""
        try:
            if not self.client:
                if not self.initialize_client():
                    st.error("Failed to initialize Google Ads client")
                    return pd.DataFrame(columns=['id', 'name', 'status'])  # Return empty DataFrame with expected columns
                
            ga_service = self.client.get_service("GoogleAdsService")
            
            query = """
                SELECT
                    campaign.id,
                    campaign.name,
                    campaign.status
                FROM campaign
                WHERE campaign.status = ENABLED
                ORDER BY campaign.name
            """
        
        # Rest of the method remains the same...
            
            search_request = self.client.get_type("SearchGoogleAdsRequest")
            search_request.customer_id = customer_id
            search_request.query = query
            
            response = ga_service.search(request=search_request)
            
            campaigns = []
            for row in response:
                campaigns.append({
                    'id': str(row.campaign.id),  # Ensure ID is string
                    'name': row.campaign.name,
                    'status': str(row.campaign.status)
                })
            
            return pd.DataFrame(campaigns)
        except Exception as e:
            st.error(f"Failed to fetch campaigns: {str(e)}")
            return pd.DataFrame(columns=['id', 'name', 'status'])  # Return empty DataFrame with expected columns
    def fetch_keywords_data(self, customer_id, start_date, end_date, campaign_ids=None):
        """Fetch keywords data for specified campaigns and date range"""
        try:
            if not self.client:
                if not self.initialize_client():
                    st.error("Google Ads client not initialized")
                    return pd.DataFrame()

            client = self.client
            ga_service = client.get_service("GoogleAdsService")
            
            # Build the query
            query = f"""
                SELECT
                    campaign.id,
                    campaign.name,
                    ad_group.id,
                    ad_group.name,
                    ad_group_criterion.keyword.text,
                    metrics.impressions,
                    metrics.clicks,
                    metrics.cost_micros,
                    metrics.conversions,
                    metrics.conversions_value,
                    metrics.ctr,
                    metrics.average_cpc
                FROM keyword_view
                WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
                AND campaign.status = ENABLED
            """
            
            # Add campaign filter if specified
            if campaign_ids:
                campaign_ids_str = ",".join([str(cid) for cid in campaign_ids])
                query += f" AND campaign.id IN ({campaign_ids_str})"
            
            # Add sorting and limiting
            query += " ORDER BY metrics.cost_micros DESC LIMIT 1000"
            
            # Execute the query
            search_request = client.get_type("SearchGoogleAdsRequest")
            search_request.customer_id = customer_id
            search_request.query = query
            
            response = ga_service.search(request=search_request)
            
            data = []
            for row in response:
                try:
                    data.append({
                        "campaign_id": row.campaign.id,
                        "campaign_name": row.campaign.name,
                        "ad_group_id": row.ad_group.id,
                        "ad_group_name": row.ad_group.name,
                        "keyword": row.ad_group_criterion.keyword.text,
                        "impressions": row.metrics.impressions,
                        "clicks": row.metrics.clicks,
                        "cost": row.metrics.cost_micros / 1000000,
                        "conversions": row.metrics.conversions,
                        "conversion_value": row.metrics.conversions_value,
                        "ctr": row.metrics.ctr,
                        "cpc": row.metrics.average_cpc,
                        "roas": (row.metrics.conversions_value / (row.metrics.cost_micros / 1000000)) 
                                if row.metrics.cost_micros > 0 else 0
                    })
                except Exception as e:
                    st.warning(f"Error processing row: {str(e)}")
                    continue
            
            if not data:
                st.warning("No keyword data returned from API")
                return pd.DataFrame()
                
            return pd.DataFrame(data)
            
        except GoogleAdsException as ex:
            st.error(f'Google Ads API Error: {ex.error.code().name}')
            st.error(f'Request ID: {ex.request_id}')
            
            for error in ex.failure.errors:
                st.error(f'- {error.message}')
                if error.location:
                    for field_path_element in error.location.field_path_elements:
                        st.error(f"\tField: {field_path_element.field_name}")
            
            return pd.DataFrame()
            
        except Exception as e:
            st.error(f"Failed to fetch keywords data: {str(e)}")
            st.error(traceback.format_exc())
            return pd.DataFrame()

@st.cache_resource
def get_ga_client():
    encoded_json = os.getenv("GA_SERVICE_ACCOUNT_JSON")
    if not encoded_json:
        raise ValueError("GA_SERVICE_ACCOUNT_JSON not set in environment variables")
    
    service_account_info = json.loads(base64.b64decode(encoded_json))
    credentials = service_account.Credentials.from_service_account_info(
        service_account_info,
        scopes=["https://www.googleapis.com/auth/analytics.readonly"]
    )
    return BetaAnalyticsDataClient(credentials=credentials)

@st.cache_resource
def get_ga_admin_client():
    try:
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=["https://www.googleapis.com/auth/analytics.edit"])
        return AnalyticsAdminServiceClient(credentials=credentials)
    except Exception as e:
        st.error(f"Failed to authenticate for Admin API: {str(e)}")
        return None

def fetch_ga4_data(property_id, start_date, end_date):
    try:
        client = get_ga_client()
        search_events = ["flexi_search_form_submit", "home_flexible_search_form", 
                        "home_luxury_search_form", "room_search_form_submit"]
        
        # Create currency specification
        currency_code = "GBP"  # Set to your property's currency
        
        requests = {
            'users': RunReportRequest(
                property=f"properties/{property_id}",
                dimensions=[Dimension(name="date")],
                metrics=[Metric(name="newUsers"), Metric(name="totalUsers")],
                date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
                currency_code=currency_code  # Add this line
            ),
            'search': RunReportRequest(
                property=f"properties/{property_id}",
                dimensions=[Dimension(name="date")],
                metrics=[Metric(name="eventCount")],
                date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
                dimension_filter=FilterExpression(filter=Filter(
                    field_name="eventName", in_list_filter=Filter.InListFilter(values=search_events))),
                currency_code=currency_code  # Add this line
            ),
            'purchase': RunReportRequest(
                property=f"properties/{property_id}",
                dimensions=[Dimension(name="date")],
                metrics=[Metric(name="eventCount"), Metric(name="purchaseRevenue")],
                date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
                dimension_filter=FilterExpression(filter=Filter(
                    field_name="eventName", string_filter=Filter.StringFilter(value="purchase"))),
                currency_code=currency_code  # Add this line
            )
        }
        
        # Rest of your function remains the same
        responses = {k: client.run_report(v) for k,v in requests.items()}
        data = {}

        for row in responses['users'].rows:
            date = row.dimension_values[0].value
            data[date] = {
                'new_users': int(row.metric_values[0].value), 
                'total_users': int(row.metric_values[1].value),
                'search_submits': 0, 
                'purchases': 0,
                'revenue': 0
            }
        
        for row in responses['search'].rows:
            date = row.dimension_values[0].value
            data.setdefault(date, {
                'new_users':0, 
                'total_users':0, 
                'search_submits':0, 
                'purchases':0,
                'revenue':0
            })
            data[date]['search_submits'] = int(row.metric_values[0].value)
        
        for row in responses['purchase'].rows:
            date = row.dimension_values[0].value
            data.setdefault(date, {
                'new_users':0, 
                'total_users':0, 
                'search_submits':0, 
                'purchases':0,
                'revenue':0
            })
            data[date]['purchases'] = int(row.metric_values[0].value)
            data[date]['revenue'] = float(row.metric_values[1].value)
        
        dates = sorted(data.keys())
        return pd.DataFrame({
            'date': pd.to_datetime(dates),
            'new_users': [data[d]['new_users'] for d in dates],
            'total_users': [data[d]['total_users'] for d in dates],
            'search_submits': [data[d]['search_submits'] for d in dates],
            'purchases': [data[d]['purchases'] for d in dates],
            'revenue': [data[d]['revenue'] for d in dates]
        })
    except Exception as e:
        st.error(f"Failed to fetch GA4 data: {str(e)}")
        st.stop()

def fetch_source_medium_data(property_id, start_date, end_date):
    try:
        client = get_ga_client()
        
        # Main request for sessions and users
        request = RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=[Dimension(name="date"), Dimension(name="sessionSourceMedium")],
            metrics=[Metric(name="sessions"), Metric(name="newUsers")],
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            limit=10000
        )
        
        # Purchase data request with revenue
        purchase_request = RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=[Dimension(name="date"), Dimension(name="sessionSourceMedium")],
            metrics=[Metric(name="eventCount"), Metric(name="purchaseRevenue")],
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
            dimension_filter=FilterExpression(
                filter=Filter(
                    field_name="eventName",
                    string_filter=Filter.StringFilter(value="purchase")
                )
            ),
            limit=10000
        )
        
        # Run both requests
        response = client.run_report(request)
        purchase_response = client.run_report(purchase_request)
        
        # Process main response
        data = []
        for row in response.rows:
            date, url = row.dimension_values[0].value, row.dimension_values[1].value
            total_users, new_users = int(row.metric_values[0].value), int(row.metric_values[1].value)
            
            data.append({
                'date': date,
                'source_medium': url,
                'sessions': total_users,
                'new_users': new_users,
                'purchases': 0,
                'revenue': 0
            })
        
        # Process purchase data
        purchase_data = {}
        for row in purchase_response.rows:
            date = row.dimension_values[0].value
            source_medium = row.dimension_values[1].value
            purchases = int(row.metric_values[0].value)
            revenue = float(row.metric_values[1].value)
            key = (date, source_medium)
            purchase_data[key] = {'purchases': purchases, 'revenue': revenue}
        
        # Merge purchase data
        for item in data:
            key = (item['date'], item['source_medium'])
            if key in purchase_data:
                item['purchases'] = purchase_data[key]['purchases']
                item['revenue'] = purchase_data[key]['revenue']
        
        return pd.DataFrame(data)
    except Exception as e:
        st.error(f"Failed to fetch source/medium data: {str(e)}")
        return pd.DataFrame()

def create_source_medium_plot(source_df, metric='sessions'):
    if source_df.empty:
        return None
        
    # Ensure the metric exists in the DataFrame
    available_metrics = ['sessions', 'new_users', 'purchases', 'revenue']
    if metric not in available_metrics:
        st.error(f"Invalid metric selected. Available options: {', '.join(available_metrics)}")
        return None
    
    if metric not in source_df.columns:
        st.error(f"Metric '{metric}' not found in data")
        return None
    
    # Group by date and source_medium
    plot_df = source_df.groupby(['date', 'source_medium'])[metric].sum().reset_index()
    plot_df['date'] = pd.to_datetime(plot_df['date'])
    
    # Get top 5 sources for the legend
    top_sources = plot_df.groupby('source_medium')[metric].sum().nlargest(5).index.tolist()
    
    # Format labels and hover data based on metric
    metric_label = {
        'sessions': 'Sessions',
        'new_users': 'New Users',
        'purchases': 'Purchases',
        'revenue': 'Revenue (£)'
    }.get(metric, metric.replace('_', ' ').title())
    
    hover_format = '{:,.0f}' if metric != 'revenue' else '£{:,.2f}'
    
    # Create the plot
    try:
        fig = px.line(plot_df[plot_df['source_medium'].isin(top_sources)],
                     x='date', y=metric, color='source_medium',
                     title=f"User Acquisition by Source/Medium ({metric_label})",
                     labels={'date': 'Date', metric: metric_label, 'source_medium': 'Source/Medium'},
                     hover_data={metric: f':{hover_format}', 'date': '%b %d, %Y'},
                     template='plotly_white')
        
        fig.update_layout(
            hovermode='x unified',
            legend_title_text='Top 5 Sources',
            legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1),
            xaxis=dict(title='Date', showgrid=False),
            plot_bgcolor='rgba(0,0,0,0)', 
            paper_bgcolor='rgba(0,0,0,0)',
            height=500
        )
        
        fig.update_traces(
            hovertemplate=f'<b>%{{fullData.name}}</b><br>Date: %{{x|%b %d, %Y}}<br>{metric_label}: %{{y:{hover_format}}}<extra></extra>',
            line=dict(width=2)
        )
        
        return fig
    except Exception as e:
        st.error(f"Failed to create plot: {str(e)}")
        return None

def fetch_detailed_url_data(property_id, start_date_str, end_date_str=None):
    end_date_str = end_date_str or start_date_str
    try:
        client = get_ga_client()
        response = client.run_report(RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=[Dimension(name="unifiedPageScreen")],
            metrics=[Metric(name="totalUsers"), Metric(name="newUsers")],
            date_ranges=[DateRange(start_date=start_date_str, end_date=end_date_str)],
            dimension_filter=FilterExpression(filter=Filter(
                field_name="unifiedPageScreen",
                string_filter=Filter.StringFilter(match_type="CONTAINS", value="/book/?rates"))),
            limit=1000))
        
        urls, total_users, new_users = [], [], []
        for row in response.rows:
            url = row.dimension_values[0].value
            if "checkin=" in url:
                urls.append(url)
                total_users.append(int(row.metric_values[0].value))
                new_users.append(int(row.metric_values[1].value))
        
        return pd.DataFrame({'URL': urls, 'Total Users': total_users, 'New Users': new_users}).sort_values('Total Users', ascending=False)
    except Exception as e:
        st.error(f"Failed to fetch detailed URL data: {str(e)}")
        return pd.DataFrame()

def fetch_specific_page_data(property_id, start_date, end_date):
    try:
        client = get_ga_client()
        response = client.run_report(RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=[Dimension(name="date"), Dimension(name="unifiedPageScreen")],
            metrics=[Metric(name="totalUsers"), Metric(name="newUsers")],
            date_ranges=[DateRange(start_date=start_date, end_date=end_date)]))
        
        data, detailed_urls = {}, {}
        for row in response.rows:
            date, url = row.dimension_values[0].value, row.dimension_values[1].value
            total_users, new_users = int(row.metric_values[0].value), int(row.metric_values[1].value)
            
            if url == "/": page_group = "/"
            elif "book/?rates=&checkin" in url: page_group = "/book/?rates=&checkin"
            elif "book/?confirm=" in url: page_group = "/book/?confirm="
            elif "complete" in url.lower(): page_group = "/complete"
            elif url in ["/book", "/book/"]: page_group = "/book"
            else: continue
            
            data.setdefault(date, {})
            detailed_urls.setdefault(date, {})
            data[date].setdefault(page_group, 0)
            detailed_urls[date].setdefault(page_group, [])
            
            data[date][page_group] += total_users
            detailed_urls[date][page_group].append({'url': url, 'total_users': total_users, 'new_users': new_users})
        
        dates = sorted(data.keys())
        page_groups = ["/", "/book", "/book/?rates=&checkin", "/book/?confirm=", "/complete"]
        df = pd.DataFrame(index=pd.to_datetime(dates), columns=page_groups).fillna(0)
        for date in dates:
            for page_group in data[date]:
                df.at[pd.to_datetime(date), page_group] = data[date][page_group]
        
        return df.reset_index().rename(columns={'index': 'date'}), detailed_urls
    except Exception as e:
        st.error(f"Failed to fetch specific page data: {str(e)}")
        st.stop()

def create_page_area_plot(df, detailed_urls, title="Total Users by Page Category"):
    plot_df = df.rename(columns={
        '/book/?rates=&checkin': 'Date Searches',
        '/book/?confirm=': 'Addons Page',
        '/complete': 'Purchases'})
    
    plot_columns = {
        '/': 'Homepage (/)',
        '/book': 'Book Page',
        'Date Searches': 'Date Searches',
        'Addons Page': 'Addons Page',
        'Purchases': 'Purchases'}
    
    available_columns = [col for col in plot_columns if col in plot_df.columns]
    df_melted = plot_df.melt(id_vars='date', value_vars=available_columns,
                           var_name='page_category', value_name='total_users')
    df_melted['page_category'] = df_melted['page_category'].map(plot_columns)
    
    color_map = {
        "Homepage (/)": "#4285F4",
        "Book Page": "#9E9E9E",
        "Date Searches": "#EA4335",
        "Addons Page": "#FBBC05",
        "Purchases": "#34A853"}
    
    # Create the plot with all categories on the same y-axis
    fig = px.line(df_melted, 
                 x='date', y='total_users', color='page_category',
                 color_discrete_map=color_map, title=title,
                 labels={'total_users': 'Total Users', 'date': 'Date'},
                 hover_data={'total_users': ':,', 'date': '%b %d, %Y'},
                 template='plotly_white')
    
    # Add purchase annotations on x-axis with font size 12
    purchases = df_melted[df_melted['page_category'] == 'Purchases']
    for idx, row in purchases.iterrows():
        if row['total_users'] > 0:  # Only annotate if there are purchases
            fig.add_annotation(
                x=row['date'],
                y=0,  # Position at bottom of chart
                text=f"★ {row['total_users']}",
                showarrow=False,
                yshift=-25,  # Position below x-axis
                font=dict(color="#34A853", size=13),  # Font size 12
                xanchor="center"
            )
    
    fig.update_layout(
        hovermode='x unified', 
        legend_title_text='Page Category',
        legend=dict(
            orientation='h',
            yanchor='bottom',
            y=1.02,
            xanchor='right',
            x=1,
            font=dict(size=14)  # Legend font size 12
        ),
        yaxis=dict(
            title='Total Users',
            showgrid=False,
            title_font=dict(size=14),  # Y-axis title font size 12
            tickfont=dict(size=14)  # Y-axis tick labels font size 12
        ),
        xaxis=dict(
            title='Date',
            showgrid=False,
            title_font=dict(size=14),  # X-axis title font size 12
            tickfont=dict(size=14)  # X-axis tick labels font size 12
        ),
        plot_bgcolor='rgba(0,0,0,0)', 
        paper_bgcolor='rgba(0,0,0,0)',
        height=600, 
        clickmode='event+select',
        margin=dict(b=80),  # Add bottom margin for annotations
        title_font=dict(size=16)  # Title slightly larger (14)
    )
    
    for trace in fig.data:
        if trace.name in plot_columns.values():
            original_category = [k for k, v in plot_columns.items() if v == trace.name][0]
            trace.customdata = df_melted[df_melted['page_category'] == trace.name].apply(
                lambda row: [row['date'].strftime('%Y-%m-%d'), original_category], axis=1).values
    
    fig.update_traces(
        hovertemplate='<b>%{fullData.name}</b><br>Date: %{x|%b %d, %Y}<br>Count: %{y:,}<extra></extra>',
        line=dict(width=2)
    )
    
    return fig

def setup_logging():
    log_dir = Path.home() / "logs"
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / "ga4_audience_debug.log"
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.handlers = []
    fh = logging.FileHandler(log_file, mode='a', encoding='utf-8')
    fh.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.info("=== NEW SESSION STARTED ===")
    logger.info(f"Log file: {log_file}")
    return logger

logger = setup_logging()

def create_ga4_audience(property_id, audience_name, checkin_date):
    try:
        logger.info("\n=== Attempting to create audience ===")
        logger.info(f"Property ID: {property_id}")
        logger.info(f"Audience Name: {audience_name}")
        logger.info(f"Check-in Date: {checkin_date}")
        
        client = get_ga_admin_client()
        if not client:
            logger.error("Failed to initialize GA Admin client")
            st.error("Failed to initialize GA Admin client")
            return None

        include_pattern = f"/book/?rates=&checkin={checkin_date}"
        exclude_pattern = "/book/?booking=&complete"
        
        inclusion_filter = AudienceFilterExpression(
            and_group=AudienceFilterExpression.AudienceFilterExpressionList(
                expressions=[AudienceFilterExpression(
                    simple_filter=AudienceSimpleFilter(
                        scope=AudienceFilterScope.PAGE_VIEW,
                        filter_expression=AudienceSimpleFilter.StringFilter(
                            match_type="CONTAINS", value=include_pattern)))]))
        
        exclusion_filter = AudienceFilterExpression(
            and_group=AudienceFilterExpression.AudienceFilterExpressionList(
                expressions=[AudienceFilterExpression(
                    simple_filter=AudienceSimpleFilter(
                        scope=AudienceFilterScope.PAGE_VIEW,
                        filter_expression=AudienceSimpleFilter.StringFilter(
                            match_type="CONTAINS", value=exclude_pattern)))]))
        
        filter_clauses = [
            AudienceFilterClause(filter=inclusion_filter, not_operation=False),
            AudienceFilterClause(filter=exclusion_filter, not_operation=True)]
        
        audience = Audience(
            display_name=audience_name,
            description=f"Users who searched for {checkin_date} but didn't complete booking",
            membership_duration_days=30,
            filter_clauses=filter_clauses)
        
        logger.info("\nMaking API call to create audience...")
        response = client.create_audience(parent=property_id, audience=audience)
        logger.info("\n=== Audience created successfully ===")
        return response
    
    except Exception as e:
        logger.error(f"\n!!! ERROR CREATING AUDIENCE !!!\n{type(e).__name__}: {str(e)}\n{traceback.format_exc()}")
        st.error(f"Failed to create audience: {str(e)}")
        st.error("Please verify:\n1. Service account has GA4 'Edit' permission\n"
               "2. Google Analytics Admin API is enabled\n"
               f"3. Correct patterns: Include: {include_pattern}, Exclude: {exclude_pattern}")
        return None
def fetch_campaign_details(client, customer_id, campaign_id):
    """Fetch all details of a specific campaign including its structure"""
    try:
        ga_service = client.get_service("GoogleAdsService")
        
        query = f"""
            SELECT
                campaign.id,
                campaign.name,
                campaign.status,
                campaign.advertising_channel_type,
                campaign.advertising_channel_sub_type,
                campaign.network_settings.target_content_network,
                campaign.network_settings.target_google_search,
                campaign.network_settings.target_search_network,
                campaign.network_settings.target_partner_search_network,
                campaign.bidding_strategy_type,
                campaign.manual_cpc.enhanced_cpc_enabled,
                campaign.start_date,
                campaign.end_date,
                campaign.campaign_budget,
                campaign_budget.amount_micros,
                campaign_budget.explicitly_shared,
                campaign_budget.period
            FROM campaign
            WHERE campaign.id = {campaign_id}
        """
        
        search_request = client.get_type("SearchGoogleAdsRequest")
        search_request.customer_id = customer_id
        search_request.query = query
        
        response = ga_service.search(request=search_request)
        
        campaign_details = {}
        for row in response:
            campaign_details = {
                'id': row.campaign.id,
                'name': row.campaign.name,
                'status': str(row.campaign.status),
                'channel_type': str(row.campaign.advertising_channel_type),
                'channel_sub_type': str(row.campaign.advertising_channel_sub_type),
                'target_content_network': row.campaign.network_settings.target_content_network,
                'target_google_search': row.campaign.network_settings.target_google_search,
                'target_search_network': row.campaign.network_settings.target_search_network,
                'target_partner_search_network': row.campaign.network_settings.target_partner_search_network,
                'bidding_strategy_type': str(row.campaign.bidding_strategy_type),
                'enhanced_cpc_enabled': row.campaign.manual_cpc.enhanced_cpc_enabled,
                'start_date': row.campaign.start_date,
                'end_date': row.campaign.end_date,
                'budget_amount': row.campaign_budget.amount_micros / 1000000,
                'budget_explicitly_shared': row.campaign_budget.explicitly_shared,
                'budget_period': str(row.campaign_budget.period),
                'budget_resource_name': row.campaign.campaign_budget
            }
        
        return campaign_details
    
    except Exception as e:
        st.error(f"Failed to fetch campaign details: {str(e)}")
        return None

def fetch_ad_group_details(client, customer_id, campaign_id):
    """Fetch all ad groups and their details for a campaign"""
    try:
        ga_service = client.get_service("GoogleAdsService")
        
        query = f"""
            SELECT
                ad_group.id,
                ad_group.name,
                ad_group.status,
                ad_group.type,
                ad_group.cpc_bid_micros,
                ad_group.target_cpa_micros,
                ad_group.target_roas,
                ad_group.target_roas_source,
                ad_group.cpv_bid_micros,
                ad_group.cpm_bid_micros,
                ad_group.percent_cpc_bid_micros,
                ad_group.effective_target_cpa_micros,
                ad_group.effective_target_cpa_source,
                ad_group.effective_target_roas,
                ad_group.effective_target_roas_source
            FROM ad_group
            WHERE campaign.id = {campaign_id}
        """
        
        search_request = client.get_type("SearchGoogleAdsRequest")
        search_request.customer_id = customer_id
        search_request.query = query
        
        response = ga_service.search(request=search_request)
        
        ad_groups = []
        for row in response:
            ad_groups.append({
                'id': row.ad_group.id,
                'name': row.ad_group.name,
                'status': str(row.ad_group.status),
                'type': str(row.ad_group.type),
                'cpc_bid_micros': row.ad_group.cpc_bid_micros,
                'target_cpa_micros': row.ad_group.target_cpa_micros,
                'target_roas': row.ad_group.target_roas,
                'target_roas_source': str(row.ad_group.target_roas_source),
                'cpv_bid_micros': row.ad_group.cpv_bid_micros,
                'cpm_bid_micros': row.ad_group.cpm_bid_micros,
                'percent_cpc_bid_micros': row.ad_group.percent_cpc_bid_micros,
                'effective_target_cpa_micros': row.ad_group.effective_target_cpa_micros,
                'effective_target_cpa_source': str(row.ad_group.effective_target_cpa_source),
                'effective_target_roas': row.ad_group.effective_target_roas,
                'effective_target_roas_source': str(row.ad_group.effective_target_roas_source)
            })
        
        return ad_groups
    
    except Exception as e:
        st.error(f"Failed to fetch ad group details: {str(e)}")
        return []

def fetch_ads_details(client, customer_id, ad_group_id):
    """Fetch all ads in an ad group"""
    try:
        ga_service = client.get_service("GoogleAdsService")
        
        query = f"""
            SELECT
                ad_group_ad.ad.id,
                ad_group_ad.ad.type,
                ad_group_ad.ad.responsive_search_ad.headlines,
                ad_group_ad.ad.responsive_search_ad.descriptions,
                ad_group_ad.ad.responsive_search_ad.path1,
                ad_group_ad.ad.responsive_search_ad.path2,
                ad_group_ad.ad.final_urls
            FROM ad_group_ad
            WHERE ad_group.id = {ad_group_id}
        """
        
        search_request = client.get_type("SearchGoogleAdsRequest")
        search_request.customer_id = customer_id
        search_request.query = query
        
        response = ga_service.search(request=search_request)
        
        ads = []
        for row in response:
            if row.ad_group_ad.ad.type == client.enums.AdTypeEnum.RESPONSIVE_SEARCH_AD:
                rsa = row.ad_group_ad.ad.responsive_search_ad
                ads.append({
                    'id': row.ad_group_ad.ad.id,
                    'type': 'RESPONSIVE_SEARCH_AD',
                    'headlines': [h.text for h in rsa.headlines],
                    'descriptions': [d.text for d in rsa.descriptions],
                    'path1': rsa.path1,
                    'path2': rsa.path2,
                    'final_urls': list(row.ad_group_ad.ad.final_urls)
                })
        
        return ads
    
    except Exception as e:
        st.error(f"Failed to fetch ad details: {str(e)}")
        return []

def create_similar_campaign(client, customer_id, base_campaign_id, new_date):
    """Create a new campaign similar to an existing one but with a new date"""
    try:
        # Fetch base campaign details
        campaign_details = fetch_campaign_details(client, customer_id, base_campaign_id)
        if not campaign_details:
            st.error("Could not fetch base campaign details")
            return None
        
        # Generate new campaign name with date
        new_campaign_name = f"{campaign_details['name'].rsplit('(', 1)[0].strip()} ({new_date})"
        
        # Get services
        campaign_service = client.get_service("CampaignService")
        campaign_budget_service = client.get_service("CampaignBudgetService")
        
        # Create budget operation
        budget_operation = client.get_type("CampaignBudgetOperation")
        budget = budget_operation.create
        budget.name = f"{new_campaign_name} Budget"
        budget.amount_micros = int(campaign_details['budget_amount'] * 1000000)
        budget.delivery_method = client.enums.BudgetDeliveryMethodEnum.STANDARD
        budget.explicitly_shared = campaign_details['budget_explicitly_shared']
        
        # Add budget operation
        budget_response = campaign_budget_service.mutate_campaign_budgets(
            customer_id=customer_id,
            operations=[budget_operation]
        )
        budget_id = budget_response.results[0].resource_name.split('/')[-1]
        
        # Create campaign operation
        campaign_operation = client.get_type("CampaignOperation")
        campaign = campaign_operation.create
        campaign.name = new_campaign_name
        campaign.advertising_channel_type = client.enums.AdvertisingChannelTypeEnum.SEARCH
        campaign.status = client.enums.CampaignStatusEnum.PAUSED  # Start paused for review
        campaign.manual_cpc.enhanced_cpc_enabled = campaign_details['enhanced_cpc_enabled']
        campaign.campaign_budget = campaign_budget_service.campaign_budget_path(
            customer_id, budget_id)
        
        # Set network settings
        campaign.network_settings.target_google_search = campaign_details['target_google_search']
        campaign.network_settings.target_search_network = campaign_details['target_search_network']
        campaign.network_settings.target_content_network = campaign_details['target_content_network']
        campaign.network_settings.target_partner_search_network = campaign_details['target_partner_search_network']
        
        # Set dates (adjust as needed)
        campaign.start_date = datetime.now().strftime('%Y%m%d')
        campaign.end_date = (datetime.now() + timedelta(days=30)).strftime('%Y%m%d')
        
        # Add campaign operation
        campaign_response = campaign_service.mutate_campaigns(
            customer_id=customer_id,
            operations=[campaign_operation]
        )
        new_campaign_id = campaign_response.results[0].resource_name.split('/')[-1]
        
        return new_campaign_id
    
    except Exception as e:
        st.error(f"Failed to create similar campaign: {str(e)}")
        st.error(traceback.format_exc())
        return None
    
def fetch_purchases_by_checkin_date(property_id, start_date_str, end_date_str):
    try:
        client = get_ga_client()
        response = client.run_report(RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=[Dimension(name="date"), Dimension(name="pageReferrer")],
            metrics=[Metric(name="eventCount"), Metric(name="purchaseRevenue")],
            date_ranges=[DateRange(start_date=start_date_str, end_date=end_date_str)],
            dimension_filter=FilterExpression(filter=Filter(
                field_name="eventName",
                string_filter=Filter.StringFilter(value="purchase"))),
            limit=10000))
        
        data = []
        for row in response.rows:
            date = row.dimension_values[0].value
            referrer = row.dimension_values[1].value
            purchases = int(row.metric_values[0].value)
            revenue = float(row.metric_values[1].value)
            
            # Extract check-in date from referrer URL
            if "checkin=" in referrer:
                checkin_date = referrer.split("checkin=")[1].split("&")[0]
                try:
                    # Validate date format
                    datetime.strptime(checkin_date, "%Y-%m-%d")
                    data.append({
                        'Check-in Date': checkin_date,
                        'Purchases': purchases,
                        'Revenue': revenue
                    })
                except ValueError:
                    continue
        
        if data:
            df = pd.DataFrame(data)
            return df.groupby('Check-in Date').agg({'Purchases': 'sum', 'Revenue': 'sum'}).reset_index()
        return pd.DataFrame()
    
    except Exception as e:
        st.error(f"Failed to fetch purchases by check-in date: {str(e)}")
        return pd.DataFrame()

def get_month_range(month_name, year):
    month_num = list(calendar.month_name).index(month_name)
    first_day = datetime(year, month_num, 1)
    
    # If this is the current month, only go up to today
    current_date = datetime.now()
    if year == current_date.year and month_num == current_date.month:
        last_day = current_date
    else:
        last_day = datetime(year, month_num, calendar.monthrange(year, month_num)[1])
    
    return first_day.strftime("%Y-%m-%d"), last_day.strftime("%Y-%m-%d")

def get_week_range(year, week_num):
    first_day = datetime.strptime(f"{year}-W{week_num-1}-1", "%Y-W%W-%w")
    last_day = first_day + timedelta(days=6)
    return first_day.strftime("%Y-%m-%d"), last_day.strftime("%Y-%m-%d")

def get_available_months():
    current_date = datetime.now()
    months = []
    for i in range(12):
        date = current_date - relativedelta(months=i)
        months.append((date.strftime("%B"), date.year))
    return months

def get_available_weeks():
    current_date = datetime.now()
    weeks = []
    for i in range(12):
        date = current_date - timedelta(weeks=i)
        week_num = date.isocalendar()[1]
        weeks.append((week_num, date.year))
    return weeks

def get_available_comparison_options(current_period_type, current_start_date, current_end_date=None):
    """Generate comparison options based on the current period selection"""
    options = []
    current_date = datetime.now()
    
    if current_period_type == "Month":
        # Previous month
        prev_month_start = (current_start_date - relativedelta(months=1)).replace(day=1)
        prev_month_end = (prev_month_start + relativedelta(months=1, days=-1))
        options.append((f"Previous Month ({prev_month_start.strftime('%b %Y')})", "previous_month"))
        
        # Same month last year
        same_month_last_year_start = (current_start_date - relativedelta(years=1)).replace(day=1)
        same_month_last_year_end = (same_month_last_year_start + relativedelta(months=1, days=-1))
        options.append((f"Same Month Last Year ({same_month_last_year_start.strftime('%b %Y')})", "same_month_last_year"))
        
    elif current_period_type == "Week":
        # Previous week
        prev_week_start = current_start_date - timedelta(weeks=1)
        prev_week_end = prev_week_start + timedelta(days=6)
        options.append((f"Previous Week ({prev_week_start.strftime('%b %d')} - {prev_week_end.strftime('%b %d')})", "previous_week"))
        
        # Same week last year
        same_week_last_year_start = current_start_date - relativedelta(years=1)
        same_week_last_year_end = same_week_last_year_start + timedelta(days=6)
        options.append((f"Same Week Last Year ({same_week_last_year_start.strftime('%b %d')} - {same_week_last_year_end.strftime('%b %d')})", "same_week_last_year"))
    
    else:  # Custom range
        if current_end_date:
            # Previous period (same length)
            days_diff = (current_end_date - current_start_date).days
            prev_start = current_start_date - timedelta(days=days_diff + 1)
            prev_end = current_start_date - timedelta(days=1)
            options.append((f"Previous {days_diff+1} Days ({prev_start.strftime('%b %d')} - {prev_end.strftime('%b %d')})", "previous_period"))
            
            # Same period last year
            last_year_start = current_start_date - relativedelta(years=1)
            last_year_end = current_end_date - relativedelta(years=1)
            options.append((f"Same Period Last Year ({last_year_start.strftime('%b %d')} - {last_year_end.strftime('%b %d')})", "same_period_last_year"))
    
    options.append(("None", "none"))
    return options

def get_date_range_for_comparison(comparison_type, current_start, current_end=None):
    """Get date range for comparison period"""
    if comparison_type == "previous_month":
        new_start = (current_start - relativedelta(months=1)).replace(day=1)
        new_end = (new_start + relativedelta(months=1, days=-1))
        return new_start.strftime("%Y-%m-%d"), new_end.strftime("%Y-%m-%d")
    
    elif comparison_type == "same_month_last_year":
        new_start = (current_start - relativedelta(years=1)).replace(day=1)
        new_end = (new_start + relativedelta(months=1, days=-1))
        return new_start.strftime("%Y-%m-%d"), new_end.strftime("%Y-%m-%d")
    
    elif comparison_type == "previous_week":
        new_start = current_start - timedelta(weeks=1)
        new_end = new_start + timedelta(days=6)
        return new_start.strftime("%Y-%m-%d"), new_end.strftime("%Y-%m-%d")
    
    elif comparison_type == "same_week_last_year":
        new_start = current_start - relativedelta(years=1)
        new_end = new_start + timedelta(days=6)
        return new_start.strftime("%Y-%m-%d"), new_end.strftime("%Y-%m-%d")
    
    elif comparison_type == "previous_period" and current_end:
        days_diff = (current_end - current_start).days
        new_start = current_start - timedelta(days=days_diff + 1)
        new_end = current_start - timedelta(days=1)
        return new_start.strftime("%Y-%m-%d"), new_end.strftime("%Y-%m-%d")
    
    elif comparison_type == "same_period_last_year" and current_end:
        new_start = current_start - relativedelta(years=1)
        new_end = current_end - relativedelta(years=1)
        return new_start.strftime("%Y-%m-%d"), new_end.strftime("%Y-%m-%d")
    
    return None, None

def compare_periods(data1, data2, period1_name, period2_name, reverse=False):
    """
    Compare two periods with proper chronological handling and date filtering
    """
    # Clean data by removing date strings
    def clean_data(df):
        return df[~df['source_medium'].str.contains(r'^\d{8,}', regex=True)]
    
    # Determine chronological order
    def get_min_date(df):
        return pd.to_datetime(df['date']).min()
    
    # Clean and aggregate data
    clean1 = clean_data(data1).groupby('source_medium')[['sessions', 'new_users', 'purchases', 'revenue']].sum().reset_index()
    clean2 = clean_data(data2).groupby('source_medium')[['sessions', 'new_users', 'purchases', 'revenue']].sum().reset_index()
    
    # Automatically determine older and newer periods
    if get_min_date(data1) < get_min_date(data2):
        older_data, older_name = clean1, period1_name
        newer_data, newer_name = clean2, period2_name
    else:
        older_data, older_name = clean2, period2_name
        newer_data, newer_name = clean1, period1_name
    
    # Merge data with proper suffixes
    comparison = pd.merge(
        older_data,
        newer_data,
        on='source_medium',
        how='outer',
        suffixes=(f'_{older_name}', f'_{newer_name}')
    ).fillna(0)
    
    # Calculate changes (newer - older)
    for metric in ['sessions', 'new_users', 'purchases', 'revenue']:
        comparison[f'{metric}_change'] = comparison[f'{metric}_{newer_name}'] - comparison[f'{metric}_{older_name}']
        comparison[f'{metric}_pct_change'] = (
            comparison[f'{metric}_change'] / 
            comparison[f'{metric}_{older_name}'].replace(0, 1)
        ) * 100
    
    # Handle reverse comparison if needed
    if reverse:
        # Invert values and swap columns
        for metric in ['sessions', 'new_users', 'purchases', 'revenue']:
            comparison[f'{metric}_change'] *= -1
            comparison[f'{metric}_pct_change'] *= -1
            comparison.rename(columns={
                f'{metric}_{older_name}': f'{metric}_{newer_name}',
                f'{metric}_{newer_name}': f'{metric}_{older_name}'
            }, inplace=True)
    
    return comparison.sort_values(f'sessions_{newer_name}', ascending=False)

def style_dataframe(df):
    # Convert all numeric columns to float to ensure proper formatting
    numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
    for col in numeric_cols:
        df[col] = df[col].astype(float)
    
    # Create formatting dictionary
    format_dict = {col: '{:,.0f}' for col in numeric_cols if '_change' not in col and col != 'revenue'}
    format_dict.update({col: '£{:,.2f}' for col in numeric_cols if '_change' not in col and col == 'revenue'})
    format_dict.update({col: '{:+,.0f}' for col in numeric_cols if '_change' in col and '_pct_change' not in col and 'revenue' not in col})
    format_dict.update({col: '£{:+,.2f}' for col in numeric_cols if '_change' in col and '_pct_change' not in col and 'revenue' in col})
    format_dict.update({col: '{:+.1f}%' for col in numeric_cols if '_pct_change' in col})
    
    # Apply styling without background gradients
    styled_df = df.style.format(format_dict)
    
    return styled_df
def initialize_session_state():
    """Initialize all session state variables with persistence"""
    # Initialize selected_account first with a default value
    if 'selected_account' not in st.session_state:
        st.session_state.selected_account = "1296045272"  # Default to Mercure Hyde Park
    
    # Initialize other required session state variables
    if 'manager_connected' not in st.session_state:
        st.session_state.manager_connected = False
    
    if 'client_connected' not in st.session_state:
        st.session_state.client_connected = False
    
    if 'manager' not in st.session_state:
        st.session_state.manager = None
    
    if 'client_manager' not in st.session_state:
        st.session_state.client_manager = None
    
    if 'ads_data' not in st.session_state:
        st.session_state.ads_data = pd.DataFrame()
    
    if 'keywords_data' not in st.session_state:
        st.session_state.keywords_data = pd.DataFrame()
def connect_manager_account():
    """Connect to the Google Ads Manager Account (MCC) with persistence"""
    with st.spinner("Authenticating with Google Ads API..."):
        try:
            # Initialize config with MCC ID
            manager_config = GoogleAdsConfig(customer_id="2101035405", is_manager=True)
            
            # First get credentials
            if not manager_config.get_credentials():
                st.error("Failed to get authentication token")
                return False
            
            # Then initialize client
            st.session_state.manager = GoogleAdsManager(manager_config)
            if st.session_state.manager.initialize_client():
                st.session_state.manager_connected = True
                st.success("✅ Successfully connected to Manager Account")
                return True
            return False
        except Exception as e:
            st.error(f"Connection failed: {str(e)}")
            st.error("Please verify:")
            st.error("1. Correct credentials in .env file")
            st.error("2. Valid developer token")
            st.error(f"3. Error details: {e}")
            return False

def connect_client_account():
    """Connect to the selected Google Ads client account with persistence"""
    account_name = google_ads_accounts.get(st.session_state.selected_account, st.session_state.selected_account)
    with st.spinner(f"Connecting to {account_name}..."):
        try:
            # Initialize config with client ID
            client_config = GoogleAdsConfig(customer_id=st.session_state.selected_account)
            
            # First get credentials
            if not client_config.get_credentials():
                st.error("Failed to get authentication token")
                return False
            
            # Then initialize client
            st.session_state.client_manager = GoogleAdsManager(client_config)
            if st.session_state.client_manager.initialize_client():
                st.session_state.client_connected = True
                st.success(f"✅ Successfully connected to {account_name}")
                return True
            return False
        except Exception as e:
            st.error(f"Connection failed: {str(e)}")
            st.error("Common issues:")
            st.error("1. MCC has access to this client account")
            st.error("2. Correct client ID in selection")
            st.error(f"3. API error: {e}")
            return False

def fetch_ads_data(start_date, end_date):
    """Fetch Google Ads data for the selected account and date range"""
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")
    
    with st.spinner(f"Fetching data for {google_ads_accounts[st.session_state.selected_account]}..."):
        try:
            # Fetch all campaigns data first
            st.session_state.ads_data = st.session_state.client_manager.fetch_google_ads_data(
                customer_id=st.session_state.selected_account,
                start_date=start_date_str,
                end_date=end_date_str
            )
            
            # If we have search campaigns, fetch keywords data
            if not st.session_state.ads_data.empty:
                search_campaigns = [campaign for campaign in st.session_state.ads_data['campaign_name'].unique() 
                                  if 'search' in campaign.lower() or 'brand' in campaign.lower()]
                
                if search_campaigns:
                    campaign_ids = st.session_state.ads_data[
                        st.session_state.ads_data['campaign_name'].isin(search_campaigns)
                    ]['campaign_id'].unique().tolist()
                    
                    st.session_state.keywords_data = st.session_state.client_manager.fetch_keywords_data(
                        customer_id=st.session_state.selected_account,
                        start_date=start_date_str,
                        end_date=end_date_str,
                        campaign_ids=campaign_ids
                    )
            
            st.success("✅ Data fetched successfully!")
            return True
        except Exception as e:
            st.error(f"Data fetch failed: {str(e)}")
            st.error("Try reconnecting or checking the date range")
            return False

def display_ads_kpis():
    """Display key performance indicators for Google Ads data"""
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.subheader("📈 Key Performance Indicators (All Campaigns)")
    ads_data = st.session_state.ads_data
    if ads_data.empty or 'cost' not in ads_data.columns:
        st.error("No valid Google Ads data available")
        st.markdown('</div>', unsafe_allow_html=True)
        return
    total_cost = ads_data['cost'].sum()
    total_clicks = ads_data['clicks'].sum()
    total_impressions = ads_data['impressions'].sum()
    total_conversions = ads_data['conversions'].sum()
    total_conversion_value = ads_data['conversion_value'].sum()
    avg_ctr = (ads_data['clicks'].sum() / ads_data['impressions'].sum()) * 100
    avg_cpc = ads_data['cost'].sum() / ads_data['clicks'].sum()
    avg_roas = ads_data['conversion_value'].sum() / ads_data['cost'].sum()
    
    kpi1, kpi2, kpi3 = st.columns(3)
    kpi4, kpi5, kpi6 = st.columns(3)
    
    kpi1.metric("Total Spend", f"£{total_cost:,.2f}")
    kpi2.metric("Total Clicks", f"{total_clicks:,}", f"CTR: {avg_ctr:.1f}%")
    kpi3.metric("Total Impressions", f"{total_impressions:,}")
    kpi4.metric("Conversions", f"{total_conversions:,}")
    kpi5.metric("Conversion Value", f"£{total_conversion_value:,.2f}", f"ROAS: {avg_roas:.1f}")
    kpi6.metric("Avg. CPC", f"£{avg_cpc:.2f}")
    st.markdown('</div>', unsafe_allow_html=True)

def display_ads_time_series(start_date, end_date):
    """Display time series charts for Google Ads performance"""
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.subheader("📅 Performance Over Time (All Campaigns)")
    
    if st.session_state.ads_data.empty:
        st.warning("No Google Ads data available")
        st.markdown('</div>', unsafe_allow_html=True)
        return
    
    if 'date' not in st.session_state.ads_data.columns:
        st.error("Date column not found in Google Ads data")
        st.markdown('</div>', unsafe_allow_html=True)
        return
    
    try:
        daily_data = st.session_state.ads_data.groupby('date').agg({
            'cost': 'sum',
            'clicks': 'sum',
            'impressions': 'sum',
            'conversions': 'sum',
            'conversion_value': 'sum'
        }).reset_index()
        
        fig = go.Figure()
        
        # Primary axis (left) - Monetary metrics
        fig.add_trace(go.Scatter(
            x=daily_data['date'], y=daily_data['cost'],
            name='Cost (£)', line=dict(color='#4285F4'),
            hovertemplate='%{x|%b %d}<br>Cost: £%{y:,.2f}<extra></extra>'
        ))
        fig.add_trace(go.Scatter(
            x=daily_data['date'], y=daily_data['conversion_value'],
            name='Conversion Value (£)', line=dict(color='#34A853'),
            hovertemplate='%{x|%b %d}<br>Value: £%{y:,.2f}<extra></extra>'
        ))
        
        # Secondary axis (right) - Count metrics
        fig.add_trace(go.Bar(
            x=daily_data['date'], y=daily_data['impressions'],
            name='Impressions', marker_color='#FBBC05',
            opacity=0.3, yaxis='y2',
            hovertemplate='%{x|%b %d}<br>Impressions: %{y:,}<extra></extra>'
        ))
        fig.add_trace(go.Bar(
            x=daily_data['date'], y=daily_data['clicks'],
            name='Clicks', marker_color='#EA4335',
            opacity=0.5, yaxis='y2',
            hovertemplate='%{x|%b %d}<br>Clicks: %{y:,}<extra></extra>'
        ))
        
        fig.update_layout(
            title=f'Performance from {start_date.strftime("%b %d")} to {end_date.strftime("%b %d")}',
            xaxis=dict(title='Date', showgrid=False),
            yaxis=dict(
                title='Cost/Value (£)',
                side='left',
                showgrid=False,
                tickprefix='£'
            ),
            yaxis2=dict(
                title='Impressions/Clicks',
                side='right',
                overlaying='y',
                showgrid=False
            ),
            legend=dict(
                orientation='h',
                yanchor='bottom',
                y=1.02,
                xanchor='right',
                x=1
            ),
            hovermode='x unified',
            plot_bgcolor='rgba(0,0,0,0)',
            height=500,
            margin=dict(l=50, r=50, t=80, b=50)
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
    except Exception as e:
        st.error(f"Error creating time series chart: {str(e)}")
    
    st.markdown('</div>', unsafe_allow_html=True)
def display_campaign_performance():
    """Display campaign performance table"""
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.subheader("📋 Enabled Campaigns Performance")
    
    # Filter for enabled campaigns only
    campaign_data = st.session_state.ads_data[
        st.session_state.ads_data['campaign_status'] == 'ENABLED'
    ] if 'campaign_status' in st.session_state.ads_data.columns else st.session_state.ads_data
    
    # Aggregate campaign data
    campaign_data = campaign_data.groupby(['campaign_id', 'campaign_name']).agg({
        'cost': 'sum',
        'impressions': 'sum',
        'clicks': 'sum',
        'conversions': 'sum',
        'conversion_value': 'sum'
    }).reset_index()
    
    # Calculate metrics
    campaign_data['ctr'] = (campaign_data['clicks'] / campaign_data['impressions']) * 100
    campaign_data['cpc'] = campaign_data['cost'] / campaign_data['clicks']
    campaign_data['roas'] = campaign_data['conversion_value'] / campaign_data['cost']
    campaign_data['cost_per_conversion'] = campaign_data['cost'] / campaign_data['conversions']
    
    # Replace infinities and NaN with 0
    campaign_data = campaign_data.replace([np.inf, -np.inf], np.nan).fillna(0)
    
    # Format the DataFrame without background_gradient if matplotlib is not available
    try:
        import matplotlib
        styled_df = (
            campaign_data.sort_values('cost', ascending=False)
            .rename(columns={
                'campaign_id': 'Campaign ID',
                'campaign_name': 'Campaign Name',
                'cost': 'Cost (£)',
                'impressions': 'Impressions',
                'clicks': 'Clicks',
                'conversions': 'Conversions',
                'conversion_value': 'Value (£)',
                'ctr': 'CTR (%)',
                'cpc': 'CPC (£)',
                'roas': 'ROAS',
                'cost_per_conversion': 'CPA (£)'
            })
            .style.format({
                'Cost (£)': '£{:,.2f}',
                'Impressions': '{:,.0f}',
                'Clicks': '{:,.0f}',
                'Conversions': '{:,.0f}',
                'Value (£)': '£{:,.2f}',
                'CTR (%)': '{:.1f}%',
                'CPC (£)': '£{:,.2f}',
                'ROAS': '{:.2f}',
                'CPA (£)': '£{:,.2f}'
            })
            .background_gradient(
                cmap='Blues',
                subset=['Cost (£)', 'Impressions']
            )
            .background_gradient(
                cmap='Greens',
                subset=['Conversions', 'Value (£)', 'ROAS']
            )
            .background_gradient(
                cmap='Reds',
                subset=['CPC (£)', 'CPA (£)'],
                vmin=0, vmax=5
            )
        )
    except ImportError:
        # Fallback without background gradients if matplotlib is not available
        styled_df = (
            campaign_data.sort_values('cost', ascending=False)
            .rename(columns={
                'campaign_id': 'Campaign ID',
                'campaign_name': 'Campaign Name',
                'cost': 'Cost (£)',
                'impressions': 'Impressions',
                'clicks': 'Clicks',
                'conversions': 'Conversions',
                'conversion_value': 'Value (£)',
                'ctr': 'CTR (%)',
                'cpc': 'CPC (£)',
                'roas': 'ROAS',
                'cost_per_conversion': 'CPA (£)'
            })
            .style.format({
                'Cost (£)': '£{:,.2f}',
                'Impressions': '{:,.0f}',
                'Clicks': '{:,.0f}',
                'Conversions': '{:,.0f}',
                'Value (£)': '£{:,.2f}',
                'CTR (%)': '{:.1f}%',
                'CPC (£)': '£{:,.2f}',
                'ROAS': '{:.2f}',
                'CPA (£)': '£{:,.2f}'
            })
        )
    
    # Display the table
    st.dataframe(
        styled_df,
        height=600,
        use_container_width=True
    )
    st.markdown('</div>', unsafe_allow_html=True)
def display_keywords_performance():
    """Display keywords performance analysis"""
    if st.session_state.keywords_data.empty:
        st.warning("No keyword data found for search campaigns")
        return
    
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.subheader("🔍 Paid Search Keywords Performance (Enabled Campaigns Only)")
    
    # Filter for enabled campaigns only
    keywords_df = st.session_state.keywords_data[
        st.session_state.keywords_data['campaign_status'] == 'ENABLED'
    ] if 'campaign_status' in st.session_state.keywords_data.columns else st.session_state.keywords_data
    
    # Rest of the function remains the same...
    keywords_df['cpc'] = np.where(
        keywords_df['clicks'] > 0,
        keywords_df['cost'] / keywords_df['clicks'],
        0
    )
    keywords_df['cost_per_conversion'] = np.where(
        keywords_df['conversions'] > 0,
        keywords_df['cost'] / keywords_df['conversions'],
        0
    )
    keywords_df['conversion_rate'] = np.where(
        keywords_df['clicks'] > 0,
        keywords_df['conversions'] / keywords_df['clicks'],
        0
    )
    
    # Identify top and bottom performers
    top_keywords = keywords_df.nlargest(10, 'conversion_value').copy()
    bottom_keywords = keywords_df[keywords_df['cost'] > 0].nsmallest(10, 'roas').copy()
    
    # Display top keywords with clicks and impressions
    st.markdown("### 🏆 Top Performing Keywords (by Conversion Value)")
    st.dataframe(
        top_keywords[['keyword', 'campaign_name', 'impressions', 'clicks', 'cost', 
                    'conversions', 'conversion_value', 'roas', 'ctr', 'cpc', 
                    'conversion_rate', 'cost_per_conversion']]
        .rename(columns={
            'impressions': 'Impressions',
            'clicks': 'Clicks',
            'cost': 'Cost (£)',
            'conversions': 'Conversions',
            'conversion_value': 'Value (£)',
            'roas': 'ROAS',
            'ctr': 'CTR',
            'cpc': 'CPC (£)',
            'conversion_rate': 'Conv. Rate',
            'cost_per_conversion': 'CPA (£)'
        })
        .style.format({
            'Impressions': '{:,.0f}',
            'Clicks': '{:,.0f}',
            'Cost (£)': '£{:,.2f}',
            'Value (£)': '£{:,.2f}',
            'ROAS': '{:.2f}',
            'CTR': '{:.2%}',
            'CPC (£)': '£{:,.2f}',
            'Conv. Rate': '{:.2%}',
            'CPA (£)': '£{:,.2f}'
        }).background_gradient(
            cmap='Blues',
            subset=['Impressions', 'Clicks']
        ).background_gradient(
            cmap='Greens',
            subset=['Value (£)', 'ROAS', 'Conv. Rate']
        ).background_gradient(
            cmap='Reds',
            subset=['CPC (£)', 'CPA (£)']
        ),
        height=400,
        use_container_width=True
    )
    
    # Display bottom keywords with clicks and impressions
    st.markdown("### 🚨 Underperforming Keywords (by ROAS)")
    st.dataframe(
        bottom_keywords[['keyword', 'campaign_name', 'impressions', 'clicks', 'cost', 
                    'conversions', 'conversion_value', 'roas', 'ctr', 'cpc',
                    'conversion_rate', 'cost_per_conversion']]
        .rename(columns={
            'impressions': 'Impressions',
            'clicks': 'Clicks',
            'cost': 'Cost (£)',
            'conversions': 'Conversions',
            'conversion_value': 'Value (£)',
            'roas': 'ROAS',
            'ctr': 'CTR',
            'cpc': 'CPC (£)',
            'conversion_rate': 'Conv. Rate',
            'cost_per_conversion': 'CPA (£)'
        })
        .style.format({
            'Impressions': '{:,.0f}',
            'Clicks': '{:,.0f}',
            'Cost (£)': '£{:,.2f}',
            'Value (£)': '£{:,.2f}',
            'ROAS': '{:.2f}',
            'CTR': '{:.2%}',
            'CPC (£)': '£{:,.2f}',
            'Conv. Rate': '{:.2%}',
            'CPA (£)': '£{:,.2f}'
        }).background_gradient(
            cmap='Blues',
            subset=['Impressions', 'Clicks']
        ).background_gradient(
            cmap='Reds',
            subset=['ROAS', 'CPA (£)'],
            vmin=0, vmax=1
        ).background_gradient(
            cmap='Greens',
            subset=['CTR', 'Conv. Rate'],
            vmin=0, vmax=0.1
        ),
        height=400,
        use_container_width=True
    )
    # Keyword trends visualization
    st.markdown("### 📈 Keyword Performance Trends")
    
    # Aggregate by keyword
    keyword_performance = keywords_df.groupby('keyword').agg({
        'impressions': 'sum',
        'clicks': 'sum',
        'cost': 'sum',
        'conversions': 'sum',
        'conversion_value': 'sum'
    }).reset_index()
    
    keyword_performance['ctr'] = keyword_performance['clicks'] / keyword_performance['impressions']
    keyword_performance['cpc'] = keyword_performance['cost'] / keyword_performance['clicks']
    keyword_performance['roas'] = keyword_performance['conversion_value'] / keyword_performance['cost']
    keyword_performance['conversion_rate'] = keyword_performance['conversions'] / keyword_performance['clicks']
    
    # Select metrics to visualize
    metric_options = ['impressions', 'clicks', 'cost', 'conversions', 'conversion_value', 
                    'ctr', 'cpc', 'roas', 'conversion_rate']
    selected_metrics = st.multiselect(
        "Select metrics to visualize",
        options=metric_options,
        default=['impressions', 'clicks', 'cost', 'roas'],
        key='keyword_metrics'
    )
    
    if selected_metrics:
        # Get top 20 keywords by impressions
        top_keywords_viz = keyword_performance.nlargest(20, 'impressions')
        
        # Create small multiples plot
        fig = px.scatter(
            top_keywords_viz,
            x='keyword',
            y=selected_metrics,
            facet_col='variable',
            facet_col_wrap=min(3, len(selected_metrics)),
            facet_col_spacing=0.1,
            height=600,
            title="Top 20 Keywords by Impressions (Search Campaigns)"
        )
        
        fig.update_layout(
            showlegend=False,
            xaxis=dict(showticklabels=False),
            xaxis2=dict(showticklabels=False),
            xaxis3=dict(showticklabels=False)
        )
        
        fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))
        st.plotly_chart(fig, use_container_width=True)
        
        # Additional visualization for clicks vs impressions
        st.markdown("### 📊 Clicks vs Impressions Analysis")
        
        # Create bubble chart showing clicks vs impressions with size as cost
        fig = px.scatter(
            keyword_performance.nlargest(50, 'impressions'),
            x='impressions',
            y='clicks',
            size='cost',
            color='roas',
            hover_name='keyword',
            log_x=True,
            log_y=True,
            size_max=30,
            color_continuous_scale='RdYlGn',
            title="Clicks vs Impressions (Size=Cost, Color=ROAS)"
        )
        
        fig.update_layout(
            xaxis_title="Impressions (log scale)",
            yaxis_title="Clicks (log scale)",
            coloraxis_colorbar=dict(title="ROAS"),
            height=600
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    st.markdown('</div>', unsafe_allow_html=True)

def display_connection_details():
    """Display connection details and status with token generation option"""
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.subheader("🔧 Connection Details")
    
    if st.session_state.manager_connected:
        config = st.session_state.manager.config
        
        st.write("##### 🔐 Authentication Status")
        cols = st.columns(2)
        with cols[0]:
            st.metric("Manager Account", "2101035405", "Connected" if st.session_state.manager_connected else "Disconnected")
        with cols[1]:
            account_name = google_ads_accounts.get(st.session_state.selected_account, st.session_state.selected_account)
            st.metric("Client Account", account_name, "Connected" if st.session_state.client_connected else "Disconnected")
        
        st.write("##### ⚙️ Configuration")
        st.json({
            "Selected Account": st.session_state.selected_account,
            "Token Status": "Valid" if hasattr(config, 'access_token') and config.access_token else "Invalid",
            "Token Expiry": config.token_expiry.isoformat() if hasattr(config, 'token_expiry') else "N/A"
        })
        
        # Show token generation UI if there are issues
        if not (hasattr(config, 'access_token') and config.access_token):
            display_token_generation_ui()
    else:
        st.warning("Manager account not connected - connection details unavailable")
        display_token_generation_ui()
    
    st.markdown('</div>', unsafe_allow_html=True)

def main():
    # Initialize session state first
    initialize_session_state()
    
    # Then proceed with the rest of your app
    st.markdown('<h1 class="dashboard-title">📊 Ecommerce Dashboard</h1>', unsafe_allow_html=True)
    
    # Initialize date_ranges with a default value
    date_ranges = []
    
    # Google Ads account information
    global google_ads_accounts
    google_ads_accounts = {
        "7711291295": "Hotel Indigo London Paddington",
        "1296045272": "Mercure Hyde Park Hotel", 
        "3787940566": "Mercure Nottingham Hotel",
        "5668854094": "Leicester Wigston",
        "3569916895": "Best Western Sheffield City Hotel",
        "1896471470": "Mercure Paddington Hotel"
    }

    # Add property selection with hotel names
    st.markdown('<div class="card">', unsafe_allow_html=True)
    property_options = {
        "308398104": "Mercure Hyde Park",
        "308376609": "Hotel Indigo Paddington", 
        "308414291": "Mercure London Paddington",
        "308386258": "Mercure Nottingham City Centre",
        "471474513": "Best Western Sheffield",
        "308381004": "Holiday Inn Leicester Wiston"
    }
    
    
    tab1, tab2 = st.tabs(["GA4 Analytics", "Google Ads Performance"])
    
    with tab1:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        selected_property = st.selectbox(
        "Select Property",
        options=list(property_options.keys()),
        format_func=lambda x: f"{x} - {property_options[x]}",
        index=0,
        help="Select the GA4 property you want to analyze"
    )
        st.markdown('</div>', unsafe_allow_html=True)
        # Get the corresponding Google Ads account for the selected property
        property_to_ads_mapping = {
            "308398104": "1296045272",  # Mercure Hyde Park
            "308376609": "7711291295",   # Hotel Indigo Paddington
            "308414291": "1896471470",   # Mercure London Paddington
            "308386258": "3787940566",   # Mercure Nottingham City Centre
            "471474513": "3569916895",   # Best Western Sheffield
            "308381004": "5668854094"    # Holiday Inn Leicester Wiston
        }
        
        ads_account_id = property_to_ads_mapping.get(selected_property)
        property_name = property_options.get(selected_property)
        
        # Main GA4 Analytics Dashboard - Date Selection
        st.subheader("Primary Time Period Selection")
        
        # Create selection widgets for primary period
        time_period = st.radio("Select time period type:", ["Month", "Week", "Custom Range"], 
                            horizontal=True, key="primary_period_type")
        
        primary_start_date = None
        primary_end_date = None
        
        if time_period == "Month":
            available_months = get_available_months()
            selected_month = st.selectbox(
                "Select month",
                options=[f"{month} {year}" for month, year in available_months],
                index=0,
                key="primary_month"
            )
            
            month_name, year = selected_month.rsplit(" ", 1)
            year = int(year)
            start_date_str, end_date_str = get_month_range(month_name, year)
            
            # Ensure we don't go beyond today for the current month
            today = datetime.now().date()
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
            if end_date > today:
                end_date_str = today.strftime("%Y-%m-%d")
            
            date_ranges.append((start_date_str, end_date_str, selected_month))
            primary_start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
            primary_end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
            
        elif time_period == "Week":
            available_weeks = get_available_weeks()
            selected_week = st.selectbox(
                "Select week",
                options=[f"Week {week} of {year}" for week, year in available_weeks],
                index=0,
                key="primary_week"
            )
            
            week_num = int(selected_week.split()[1])
            year = int(selected_week.split()[-1])
            start_date_str, end_date_str = get_week_range(year, week_num)
            
            # Ensure we don't go beyond today for the current week
            today = datetime.now().date()
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
            if end_date > today:
                end_date_str = today.strftime("%Y-%m-%d")
            
            date_ranges.append((start_date_str, end_date_str, selected_week))
            primary_start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
            primary_end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
            
        else:  # Custom Range
            col1, col2 = st.columns(2)
            with col1:
                start_date = st.date_input("Start date", 
                                        datetime.now() - timedelta(days=30), 
                                        min_value=datetime(2020, 1, 1),
                                        key="custom_start")
            with col2:
                end_date = st.date_input("End date", 
                                    datetime.now(), 
                                    min_value=datetime(2020, 1, 1),
                                    key="custom_end")
            
            if end_date < start_date:
                st.error("Error: End date must be after start date.")
                st.stop()
                
            date_ranges.append((start_date.strftime("%Y-%m-%d"), 
                            end_date.strftime("%Y-%m-%d"), 
                            f"{start_date.strftime('%b %d, %Y')} to {end_date.strftime('%b %d, %Y')}"))
            primary_start_date = datetime.combine(start_date, datetime.min.time())
            primary_end_date = datetime.combine(end_date, datetime.min.time())

        # Now that date_ranges is populated, we can show the ROI metrics card
        if ads_account_id and date_ranges:
            display_roi_metrics_card(
                property_id=selected_property,
                property_name=property_name,
                ads_account_id=ads_account_id,
                start_date=date_ranges[0][0],  # Use the start date from the primary period
                end_date=date_ranges[0][1]     # Use the end date from the primary period
            )
        else:
            st.warning("No matching Google Ads account found for this property")
        
        # Comparison period selection
        st.subheader("Comparison Period")
        
        # Get comparison options based on primary selection
        if primary_start_date:
            comparison_options = get_available_comparison_options(
                time_period, primary_start_date, primary_end_date)
            
            comparison_choice = st.selectbox(
                "Compare with:",
                options=[opt[0] for opt in comparison_options],
                index=len(comparison_options)-1,  # Default to "None"
                key="comparison_choice"
            )
            
            # Get the comparison type key
            comparison_type = None
            for opt in comparison_options:
                if opt[0] == comparison_choice:
                    comparison_type = opt[1]
                    break

            # If a comparison is selected, add it to date_ranges
            if comparison_type and comparison_type != "none":
                comp_start, comp_end = get_date_range_for_comparison(
                    comparison_type, primary_start_date, primary_end_date)
                
                if comp_start and comp_end:
                    # Ensure comparison end date doesn't go beyond today
                    today = datetime.now().date()
                    comp_end_date = datetime.strptime(comp_end, "%Y-%m-%d").date()
                    if comp_end_date > today:
                        comp_end = today.strftime("%Y-%m-%d")
                    
                    date_ranges.append((comp_start, comp_end, comparison_choice))

        if 'clicked_point' not in st.session_state:
            st.session_state.clicked_point = None

        try:
            all_data = {}
            for start_date_str, end_date_str, period_name in date_ranges:
                with st.spinner(f"Fetching analytics data for {period_name}..."):
                    ga4_data = fetch_ga4_data(selected_property, start_date_str, end_date_str)
                    page_category_data, detailed_urls = fetch_specific_page_data(selected_property, start_date_str, end_date_str)
                    source_medium_data = fetch_source_medium_data(selected_property, start_date_str, end_date_str)
                    
                    all_data[period_name] = {
                        'ga4_data': ga4_data,
                        'page_category_data': page_category_data,
                        'detailed_urls': detailed_urls,
                        'source_medium_data': source_medium_data
                    }

            # Display KPIs for each period
            st.subheader("Key Metrics Comparison")
            
            if len(date_ranges) == 1:
                # Single period view
                period_name = date_ranges[0][2]
                data = all_data[period_name]['ga4_data']
                
                total_new_users = data['new_users'].sum()
                total_all_users = data['total_users'].sum()
                total_searches = data['search_submits'].sum()
                total_purchases = data['purchases'].sum()
                total_revenue = data['revenue'].sum()
                
                max_new_users = data['new_users'].max()
                max_new_users_date = data.loc[data['new_users'].idxmax(), 'date'].strftime('%b %d')
                max_purchases = data['purchases'].max()
                max_purchases_date = data.loc[data['purchases'].idxmax(), 'date'].strftime('%b %d') if max_purchases > 0 else "N/A"
                max_revenue = data['revenue'].max()
                max_revenue_date = data.loc[data['revenue'].idxmax(), 'date'].strftime('%b %d') if max_revenue > 0 else "N/A"
                
                kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)
                kpi1.metric("Total Users", f"{total_all_users:,}", f"↑ {total_new_users:,} new")
                kpi2.metric("New Users", f"{total_new_users:,}", f"Peak: {max_new_users:,} on {max_new_users_date}")
                kpi3.metric("Search Submits", f"{total_searches:,}")
                kpi4.metric("Purchases", f"{total_purchases:,}", f"Peak: {max_purchases:,} on {max_purchases_date}" if max_purchases > 0 else "")
                kpi5.metric("Total Revenue", f"£{total_revenue:,.2f}", f"Peak: £{max_revenue:,.2f} on {max_revenue_date}" if max_revenue > 0 else "")
                
            else:
                # Comparison view
                main_period_name = date_ranges[0][2]
                comparison_period_name = date_ranges[1][2]
                main_data = all_data[main_period_name]['ga4_data']
                comparison_data = all_data[comparison_period_name]['ga4_data']
                
                # Calculate metrics for both periods
                main_metrics = {
                    'total_users': main_data['total_users'].sum(),
                    'new_users': main_data['new_users'].sum(),
                    'search_submits': main_data['search_submits'].sum(),
                    'purchases': main_data['purchases'].sum(),
                    'revenue': main_data['revenue'].sum()
                }
                
                comparison_metrics = {
                    'total_users': comparison_data['total_users'].sum(),
                    'new_users': comparison_data['new_users'].sum(),
                    'search_submits': comparison_data['search_submits'].sum(),
                    'purchases': comparison_data['purchases'].sum(),
                    'revenue': comparison_data['revenue'].sum()
                }
                
                # Create comparison deltas
                comparison_deltas = {}
                for key in main_metrics:
                    delta = main_metrics[key] - comparison_metrics[key]
                    pct_change = (delta / comparison_metrics[key]) * 100 if comparison_metrics[key] != 0 else float('inf')
                    comparison_deltas[key] = {
                        'delta': delta,
                        'pct_change': pct_change
                    }
                
                # Display metrics in columns
                col1, col2, col3, col4, col5 = st.columns(5)
                
                with col1:
                    st.metric(
                        "Total Users",
                        f"{main_metrics['total_users']:,}",
                        delta=f"{comparison_deltas['total_users']['delta']:,} ({comparison_deltas['total_users']['pct_change']:.1f}%)",
                        help=f"{comparison_period_name}: {comparison_metrics['total_users']:,}"
                    )
                
                with col2:
                    st.metric(
                        "New Users",
                        f"{main_metrics['new_users']:,}",
                        delta=f"{comparison_deltas['new_users']['delta']:,} ({comparison_deltas['new_users']['pct_change']:.1f}%)",
                        help=f"{comparison_period_name}: {comparison_metrics['new_users']:,}"
                    )
                
                with col3:
                    st.metric(
                        "Search Submits",
                        f"{main_metrics['search_submits']:,}",
                        delta=f"{comparison_deltas['search_submits']['delta']:,} ({comparison_deltas['search_submits']['pct_change']:.1f}%)",
                        help=f"{comparison_period_name}: {comparison_metrics['search_submits']:,}"
                    )
                
                with col4:
                    st.metric(
                        "Purchases",
                        f"{main_metrics['purchases']:,}",
                        delta=f"{comparison_deltas['purchases']['delta']:,} ({comparison_deltas['purchases']['pct_change']:.1f}%)",
                        help=f"{comparison_period_name}: {comparison_metrics['purchases']:,}"
                    )
                
                with col5:
                    # Calculate delta and percentage change
                    delta = main_metrics['revenue'] - comparison_metrics['revenue']
                    pct_change = (delta / comparison_metrics['revenue']) * 100 if comparison_metrics['revenue'] != 0 else 0
                    
                    # Format the delta display
                    delta_formatted = f"{delta:,.2f} ({pct_change:.1f}%)"
                    
                    st.metric(
                        "Total Revenue",
                        f"£{main_metrics['revenue']:,.2f}",
                        delta=delta_formatted,
                        help=f"{comparison_period_name}: £{comparison_metrics['revenue']:,.2f}"
                    )
                
                # Add a caption explaining the comparison
                st.caption(f"Showing {main_period_name} metrics with changes compared to {comparison_period_name}. "
                          f"Positive values indicate improvement over the comparison period.")            
            
            # Display page category data
            st.subheader("Page Category Performance")
            
            if len(date_ranges) == 1:
                # Single period view
                period_name = date_ranges[0][2]
                page_category_data = all_data[period_name]['page_category_data']
                detailed_urls = all_data[period_name]['detailed_urls']
                
                fig = create_page_area_plot(page_category_data, detailed_urls,
                                          f"Total Users by Page Category: {period_name}")
                st.plotly_chart(fig, use_container_width=True)
            else:
                # Comparison view - show both periods side by side
                col1, col2 = st.columns(2)
                with col1:
                    period1_name = date_ranges[0][2]
                    page_category_data = all_data[period1_name]['page_category_data']
                    detailed_urls = all_data[period1_name]['detailed_urls']
                    
                    fig = create_page_area_plot(page_category_data, detailed_urls,
                                              f"Page Categories: {period1_name}")
                    st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    period2_name = date_ranges[1][2]
                    page_category_data = all_data[period2_name]['page_category_data']
                    detailed_urls = all_data[period2_name]['detailed_urls']
                    
                    fig = create_page_area_plot(page_category_data, detailed_urls,
                                              f"Page Categories: {period2_name}")
                    st.plotly_chart(fig, use_container_width=True)
            
            # Source/Medium Analysis
            st.subheader("User Acquisition by Source/Medium")
            
            if len(date_ranges) == 1:
                # Single period view
                period_name = date_ranges[0][2]
                source_medium_data = all_data[period_name]['source_medium_data']
                
                if not source_medium_data.empty:
                    metric = st.selectbox(
                        "Select metric to view by source/medium",
                        options=['sessions', 'new_users', 'purchases', 'revenue'],
                        format_func=lambda x: {
                            'sessions': 'Sessions',
                            'new_users': 'New Users',
                            'purchases': 'Purchases',
                            'revenue': 'Total Revenue'
                        }.get(x, x),
                        key='source_metric_single'
                    )
                    
                    fig = create_source_medium_plot(source_medium_data, metric)
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Show a table with the top sources
                    st.subheader("Top Sources Summary")
                    top_sources = source_medium_data.groupby('source_medium').agg({
                        'sessions': 'sum',
                        'new_users': 'sum',
                        'purchases': 'sum',
                        'revenue': 'sum'
                    }).sort_values('sessions', ascending=False).head(10)
                    
                    st.dataframe(
                        top_sources.style.format({
                            'sessions': '{:,}',
                            'new_users': '{:,}',
                            'purchases': '{:,}',
                            'revenue': '£{:,.2f}'
                        }).background_gradient(cmap='Blues'),
                        use_container_width=True
                    )
                else:
                    st.warning("No source/medium data available for the selected date range.")
                    
            else:
                period1_name = date_ranges[0][2]
                period2_name = date_ranges[1][2]
                source_medium_data1 = all_data[period1_name]['source_medium_data']
                source_medium_data2 = all_data[period2_name]['source_medium_data']
                
                if not source_medium_data1.empty and not source_medium_data2.empty:
                    metric = st.selectbox(
                        "Select metric to compare",
                        options=['sessions', 'new_users', 'purchases', 'revenue'],
                        format_func=lambda x: {
                            'sessions': 'Sessions',
                            'new_users': 'New Users',
                            'purchases': 'Purchases',
                            'revenue': 'Total Revenue'
                        }.get(x, x),
                        key='source_metric_compare'
                    )
                    
                    # Create comparison visualization
                    agg_data1 = source_medium_data1.groupby('source_medium')[metric].sum().reset_index()
                    agg_data1['period'] = period1_name
                    
                    agg_data2 = source_medium_data2.groupby('source_medium')[metric].sum().reset_index()
                    agg_data2['period'] = period2_name
                    
                    combined = pd.concat([agg_data1, agg_data2])
                    pivot_data = combined.pivot(index='source_medium', columns='period', values=metric)
                    pivot_data = pivot_data.fillna(0)
                    pivot_data['change'] = pivot_data[period1_name] - pivot_data[period2_name]
                    pivot_data['pct_change'] = (pivot_data['change'] / pivot_data[period2_name]) * 100
                    pivot_data = pivot_data.replace([np.inf, -np.inf], np.nan)
                    
                    top_sources = pivot_data.nlargest(10, period1_name).index.tolist()
                    filtered_data = pivot_data.loc[top_sources]
                    
                    # Create visualization
                    fig = go.Figure()
                    
                    # Format numbers based on metric
                    if metric == 'revenue':
                        text_format = '£{:,.2f}'
                        hover_format = '£{:,.2f}'
                    else:
                        text_format = '{:,.0f}'
                        hover_format = '{:,.0f}'
                    
                    fig.add_trace(go.Bar(
                        x=filtered_data.index,
                        y=filtered_data[period1_name],
                        name=period1_name,
                        marker_color=px.colors.qualitative.Plotly[0],
                        text=filtered_data[period1_name].apply(lambda x: text_format.format(x)),
                        textposition='auto',
                        hovertemplate='%{x}<br>' + period1_name + ': ' + hover_format + '<extra></extra>'
                    ))
                    
                    fig.add_trace(go.Bar(
                        x=filtered_data.index,
                        y=filtered_data[period2_name],
                        name=period2_name,
                        marker_color=px.colors.qualitative.Plotly[1],
                        text=filtered_data[period2_name].apply(lambda x: text_format.format(x)),
                        textposition='auto',
                        opacity=0.6,
                        hovertemplate='%{x}<br>' + period2_name + ': ' + hover_format + '<extra></extra>'
                    ))
                    
                    fig.add_trace(go.Scatter(
                        x=filtered_data.index,
                        y=filtered_data[period1_name] * 1.05,
                        mode='markers+text',
                        text=filtered_data['pct_change'].apply(lambda x: f"{x:+.1f}%" if not pd.isna(x) else "N/A"),
                        textposition='top center',
                        marker=dict(
                            color=filtered_data['pct_change'].apply(
                                lambda x: 'green' if x > 0 else 'red' if x < 0 else 'gray'),
                            size=10
                        ),
                        name='% Change',
                        hoverinfo='skip'
                    ))
                    
                    fig.update_layout(
                        title=f"<b>Top Sources Comparison: {period1_name} vs {period2_name}</b>",
                        title_x=0.05,
                        title_font_size=16,
                        barmode='group',
                        xaxis=dict(
                            title='Source/Medium',
                            tickangle=45,
                            tickfont=dict(size=12)
                        ),
                        yaxis=dict(
                            title='Revenue (£)' if metric == 'revenue' else metric.replace('_', ' ').title(),
                            gridcolor='rgba(0,0,0,0.1)'
                        ),
                        plot_bgcolor='rgba(0,0,0,0)',
                        paper_bgcolor='rgba(0,0,0,0)',
                        legend=dict(
                            orientation='h',
                            yanchor='bottom',
                            y=1.02,
                            xanchor='right',
                            x=1
                        ),
                        hovermode='x unified',
                        height=600,
                        margin=dict(l=50, r=50, t=100, b=150)
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Detailed comparison table
                    st.subheader("Detailed Source/Medium Performance")
                    
                    filtered_data = filtered_data.sort_values(period1_name, ascending=False)
                    filtered_data['change_abs'] = filtered_data['change']
                    filtered_data['change_pct'] = filtered_data['pct_change']
                    
                    styled_df = filtered_data[[period1_name, period2_name, 'change_abs', 'change_pct']].reset_index()
                    styled_df.columns = ['Source/Medium', period1_name, period2_name, 'Change', '% Change']
                    
                    def color_change(val):
                        if isinstance(val, str):
                            return ''
                        color = 'green' if val > 0 else 'red' if val < 0 else 'gray'
                        return f'color: {color}'
                    
                    # Format numbers based on metric
                    if metric == 'revenue':
                        format_dict = {
                            period1_name: '£{:,.2f}',
                            period2_name: '£{:,.2f}',
                            'Change': '£{:+,.2f}',
                            '% Change': '{:+.1f}%'
                        }
                    else:
                        format_dict = {
                            period1_name: '{:,.0f}',
                            period2_name: '{:,.0f}',
                            'Change': '{:+,.0f}',
                            '% Change': '{:+.1f}%'
                        }
                    
                    st.dataframe(
                        styled_df.style
                        .format(format_dict)
                        .applymap(color_change, subset=['Change', '% Change'])
                        .background_gradient(
                            cmap='Blues',
                            subset=[period1_name, period2_name]
                        ),
                        height=600,
                        use_container_width=True
                    )
                    
                    # Small multiple charts view
                    st.subheader("Trend Comparison by Top Sources")
                    
                    plot_df1 = source_medium_data1[source_medium_data1['source_medium'].isin(top_sources)]
                    plot_df1 = plot_df1.groupby(['date', 'source_medium'])[metric].sum().reset_index()
                    plot_df1['date'] = pd.to_datetime(plot_df1['date'])
                    plot_df1['period'] = period1_name
                    
                    plot_df2 = source_medium_data2[source_medium_data2['source_medium'].isin(top_sources)]
                    plot_df2 = plot_df2.groupby(['date', 'source_medium'])[metric].sum().reset_index()
                    plot_df2['date'] = pd.to_datetime(plot_df2['date'])
                    plot_df2['period'] = period2_name
                    
                    combined = pd.concat([plot_df1, plot_df2])
                    
                    fig = px.line(
                        combined,
                        x='date',
                        y=metric,
                        color='period',
                        facet_col='source_medium',
                        facet_col_wrap=3,
                        title=f"<b>{'Revenue' if metric == 'revenue' else metric.replace('_', ' ').title()} Trends by Source</b>",
                        labels={metric: 'Revenue (£)' if metric == 'revenue' else metric.replace('_', ' ').title(), 'date': 'Date'},
                        height=800,
                        template='plotly_white'
                    )
                    
                    fig.update_layout(
                        hovermode='x unified',
                        showlegend=True,
                        legend_title_text='Period',
                        plot_bgcolor='rgba(0,0,0,0)',
                        paper_bgcolor='rgba(0,0,0,0)'
                    )
                    
                    fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))
                    
                    st.plotly_chart(fig, use_container_width=True)
                    
                else:
                    st.warning("No source/medium data available for comparison.")

            # Detailed URL analysis
            if len(date_ranges) >= 1:
                period_name = date_ranges[0][2]
                start_date_str = date_ranges[0][0]
                end_date_str = date_ranges[0][1]
                
                with st.form("detailed_url_form"):
                    selected_category = st.selectbox("Select a page category",
                        options=["/", "/book", "/book/?rates=&checkin", "/book/?confirm=", "/complete"],
                        format_func=lambda x: {
                            "/": "Homepage (/)",
                            "/book": "Book Page",
                            "/book/?rates=&checkin": "Date Searches",
                            "/book/?confirm=": "Addons Page",
                            "/complete": "Purchases"}.get(x, x))
                    
                    submitted = st.form_submit_button("Show Detailed URLs")
                    
                if submitted and selected_category == "/book/?rates=&checkin":
                    st.markdown(f"### Stay Dates Searched in {period_name}")
                    
                    with st.spinner(f"Fetching detailed URLs from {start_date_str} to {end_date_str}..."):
                        url_details = fetch_detailed_url_data("308398104", start_date_str, end_date_str)
                        
                        if not url_details.empty:
                            url_details['Check-in Date'] = url_details['URL'].str.extract(r'checkin=(\d{4}-\d{2}-\d{2})')
                            url_details = url_details.dropna(subset=['Check-in Date'])
                            
                            if not url_details.empty:
                                try:
                                    url_details['Check-in Date'] = pd.to_datetime(url_details['Check-in Date'], format='%Y-%m-%d', errors='coerce')
                                    today = pd.to_datetime(datetime.now().date())
                                    url_details = url_details[url_details['Check-in Date'] > today]
                                    
                                    if url_details.empty:
                                        st.info("No future stay dates found in the URLs for the selected date range.")
                                        return
                                        
                                    with st.spinner("Fetching purchases data for these stay dates..."):
                                        purchases_data = fetch_purchases_by_checkin_date("308398104", start_date_str, end_date_str)
                                        
                                    url_details['Formatted Date'] = url_details['Check-in Date'].dt.strftime('%b %d, %Y')
                                    checkin_stats = url_details.groupby(['Check-in Date', 'Formatted Date']).agg(
                                        {'Total Users': 'sum', 'New Users': 'sum'}).reset_index()
                                        
                                    if not purchases_data.empty:
                                        purchases_data['Check-in Date'] = pd.to_datetime(purchases_data['Check-in Date'])
                                        checkin_stats = checkin_stats.merge(
                                            purchases_data, 
                                            on='Check-in Date', 
                                            how='left'
                                        ).fillna(0)
                                    else:
                                        checkin_stats['Purchases'] = 0
                                        checkin_stats['Revenue'] = 0.0
                                        
                                    total_users = checkin_stats['Total Users'].sum()
                                    new_users = checkin_stats['New Users'].sum()
                                    unique_stay_dates = len(checkin_stats)
                                    total_purchases = checkin_stats['Purchases'].sum()
                                    total_revenue = checkin_stats['Revenue'].sum()
                                    
                                    if not checkin_stats.empty:
                                        most_popular = checkin_stats.loc[checkin_stats['Total Users'].idxmax()]
                                        popular_date = most_popular['Formatted Date']
                                        popular_date_iso = most_popular['Check-in Date'].strftime('%Y-%m-%d')
                                        popular_users = most_popular['Total Users']

                                        col1, col2, col3, col4, col5 = st.columns(5)
                                        cols = [col1, col2, col3, col4, col5]
                                        titles = ["Total Users", "New Users", "Unique Dates", "Most Popular", "Total Purchases", "Total Revenue"]
                                        values = [f"{total_users:,}", f"{new_users:,}", f"{unique_stay_dates:,}", popular_date, f"{total_purchases:,}", f"£{total_revenue:,.2f}"]
                                        descs = ["Users viewing future stay dates", "First-time users viewing future dates", 
                                                "Future stay dates searched", f"{popular_users:,} users", "Purchases for these dates", "Revenue from these dates"]
                                        colors = ["#4285F4", "#EA4335", "#FBBC05", "#34A853", "#9E9E9E"]
                                        
                                        for col, title, value, desc, color in zip(cols, titles, values, descs, colors):
                                            with col:
                                                st.markdown(f"""
                                                <div style="padding: 20px; border-radius: 10px; border-left: 5px solid {color};
                                                    background-color: white; box-shadow: 0 4px 6px rgba(0,0,0,0.1);">
                                                    <h3 style="color: {color}; margin-top: 0;">{title}</h3>
                                                    <p style="font-size: 24px; font-weight: bold;">{value}</p>
                                                    <p style="color: #666; font-size: 12px;">{desc}</p>
                                                </div>""", unsafe_allow_html=True)
                                        
                                        st.markdown("---")
                                        st.subheader(f"Create GA4 Audience for {popular_date}")
                                        audience_name = f"Users viewing stays on {popular_date}"
                                        
                                        st.markdown(f"""
                                        **Audience Criteria:**
                                        ### Include users when:
                                        - Page URL contains: `/book/?rates=&checkin={popular_date_iso}`
                                        ### Exclude users when:
                                        - Page URL contains: `/book/?booking=&complete`""")
                                        
                                        if st.button("📌 Create Audience in GA4"):
                                            logger.info("=== Create Audience button clicked ===")
                                            with st.spinner(f"Creating audience '{audience_name}'..."):
                                                response = create_ga4_audience(
                                                    property_id="properties/308398104",
                                                    audience_name=audience_name,
                                                    checkin_date=popular_date_iso)
                                                
                                                if response:
                                                    st.success("✅ Audience created successfully!")
                                                    st.json({
                                                        "name": response.name,
                                                        "display_name": response.display_name,
                                                        "description": response.description,
                                                        "filters": {
                                                            "include": f"page_location contains '/book/?rates=&checkin={popular_date_iso}'",
                                                            "exclude": "page_location contains '/book/?booking=&complete'"
                                                        }})
                                
                                    example_urls = url_details.groupby('Check-in Date')['URL'].first().reset_index()
                                    checkin_stats = checkin_stats.merge(example_urls, on='Check-in Date')
                                    
                                    st.dataframe(
                                        checkin_stats[['Formatted Date', 'Total Users', 'New Users', 'Purchases', 'Revenue']]
                                        .rename(columns={'Formatted Date': 'Check-in Date'})
                                        .sort_values('Total Users', ascending=False)
                                        .style.format({
                                            'Total Users': '{:,}', 
                                            'New Users': '{:,}',
                                            'Purchases': '{:,}',
                                            'Revenue': '£{:,.2f}'})
                                        .background_gradient(cmap='Blues', subset=['Total Users', 'New Users'])
                                        .background_gradient(cmap='Greens', subset=['Purchases', 'Revenue']),
                                        height=600, use_container_width=True)
                                    
                                except Exception as e:
                                    st.error(f"Error processing dates: {str(e)}")

            # Raw data expander
            st.markdown('<div class="card">', unsafe_allow_html=True)
            st.subheader("Raw Data")
            if len(date_ranges) == 1:
                period_name = date_ranges[0][2]
                ga4_data = all_data[period_name]['ga4_data']
                st.dataframe(ga4_data.style.format({
                    'new_users': '{:,}', 
                    'total_users': '{:,}',
                    'search_submits': '{:,}', 
                    'purchases': '{:,}',
                    'revenue': '£{:,.2f}'
                }))
            else:
                tab1, tab2 = st.tabs([date_ranges[0][2], date_ranges[1][2]])
                with tab1:
                    ga4_data = all_data[date_ranges[0][2]]['ga4_data']
                    st.dataframe(ga4_data.style.format({
                        'new_users': '{:,}', 
                        'total_users': '{:,}',
                        'search_submits': '{:,}', 
                        'purchases': '{:,}',
                        'revenue': '£{:,.2f}'
                    }))
                with tab2:
                    ga4_data = all_data[date_ranges[1][2]]['ga4_data']
                    st.dataframe(ga4_data.style.format({
                        'new_users': '{:,}', 
                        'total_users': '{:,}',
                        'search_submits': '{:,}', 
                        'purchases': '{:,}',
                        'revenue': '£{:,.2f}'
                    }))
            st.markdown('</div>', unsafe_allow_html=True)
                
        except Exception as e:
            st.error(f"Application error: {str(e)}")

        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.info("""
        **Dashboard Features:**
        - Compare metrics across different time periods (months, weeks, or custom ranges)
        - Interactive area chart showing page categories (using Total Users)
        - User acquisition by source/medium (sessions, new users, purchases, revenue)
        - Select a date and category to see detailed URLs for that date
        - Table showing top pages by Total Users with option to view for specific dates
        - Key performance indicators with percentage change comparisons
        - Hover tooltips with detailed information
        - Clean, modern visualization style
        - Responsive layout for all device sizes""")
        st.markdown('</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)  # Close the main card

    # Tab2 (Google Ads Performance) would go here...
    
    with tab2:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.header("Google Ads Performance")

        # Google Ads account selection
        st.session_state.selected_account = st.selectbox(
            "Select Google Ads Account",
            options=list(google_ads_accounts.keys()),
            format_func=lambda x: f"{google_ads_accounts[x]} ({x})",
            index=1  # Default to Mercure Hyde Park
        )
        
        # Date range selection
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("Start date", 
                                    datetime.now() - timedelta(days=30), 
                                    min_value=datetime(2025, 4, 1),
                                    key="ads_start_date")
        with col2:
            end_date = st.date_input("End date", 
                                    datetime.now(), 
                                    min_value=datetime(2025, 5, 1),
                                    key="ads_end_date")

        # Automatic connection if tokens exist
        if not st.session_state.manager_connected:
            if st.button("🔌 Connect Manager Account (MCC)"):
                connect_manager_account()
        else:
            st.success("✅ Manager account connected (persistent session)")

        if st.session_state.manager_connected and not st.session_state.client_connected:
            if st.button(f"🔗 Connect to {google_ads_accounts[st.session_state.selected_account]}"):
                connect_client_account()
        elif st.session_state.client_connected:
            st.success(f"✅ {google_ads_accounts[st.session_state.selected_account]} connected (persistent session)")

        # Data fetching and display
        if st.session_state.client_connected:
            if st.button("📊 Fetch Google Ads Data", key="fetch_data_btn"):
                if fetch_ads_data(start_date, end_date):
                    display_ads_kpis()
                    display_ads_time_series(start_date, end_date)
                    display_campaign_performance()
                    display_keywords_performance()
        
        # Always show connection details
        display_connection_details()
        st.markdown('</div>', unsafe_allow_html=True)  # Close main card

if __name__ == "__main__":
    main()
