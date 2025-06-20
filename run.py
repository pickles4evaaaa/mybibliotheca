from app import create_app

# This app is intended to be run via Gunicorn only
app = create_app()
if __name__ == '__main__':
    raise RuntimeError("❌ To run bibliotheca, use Gunicorn:\n\n    gunicorn -w NUMBER_OF_WORKERS -b 0.0.0.0:5054 run:app\n\nNote: Replace NUMBER_OF_WORKERS with the number of worker processes you want to run. A good starting point is 4 workers but most modern machines can handle more.")
