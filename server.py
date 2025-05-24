from waitress import serve
from hotel_analytics_dashboard import main  # Import your Streamlit app

# Create a WSGI app entry point
def app(environ, start_response):
    main()  # This may need adjustment based on your app structure
    start_response('200 OK', [('Content-Type', 'text/html')])
    return [b"Streamlit App Running"]

if __name__ == '__main__':
    serve(app, host='0.0.0.0', port=8001)
