# routes/market_data.py
from flask_restx import Namespace, Resource, fields
from flask_jwt_extended import jwt_required
from flask import current_app
from services import market_overview_service # Import the service

market_overview_ns = Namespace('market_overview', description='Market overview operations')

# Define response model for Market Overview
market_overview_model = market_overview_ns.model('MarketOverview', {
    'date': fields.String(required=True, description='Persian date (YYYY/MM/DD)'),
    'shakhes_kol': fields.Nested(market_overview_ns.model('MarketIndex', {
        'value': fields.Float, 'change': fields.Float, 'percent': fields.Float, 'date': fields.String
    }), description='Overall Index'),
    'shakhes_hamvazn': fields.Nested(market_overview_ns.model('MarketIndex', {
        'value': fields.Float, 'change': fields.Float, 'percent': fields.Float, 'date': fields.String
    }), description='Equal-weighted Index'),
    'farabourse': fields.Nested(market_overview_ns.model('FarabourseIndexData', {
        'value': fields.Float, 'change': fields.Float, 'percent': fields.Float, 'date': fields.String
    }), description='Farabourse Index'),
    'shakhes_sanat': fields.Nested(market_overview_ns.model('IndustryIndexData', {
        'value': fields.Float, 'change': fields.Float, 'percent': fields.Float, 'date': fields.String
    }), description='Industry Index'),
    'commodities': fields.Raw(description='Dictionary of commodity prices (e.g., gold, silver, platinum, copper)') # Use Raw for flexible dict
})

@market_overview_ns.route('/')
class MarketOverviewResource(Resource):
    @market_overview_ns.doc(security='Bearer Auth')
    @jwt_required() # Ensure JWT is required for this endpoint
    @market_overview_ns.marshal_with(market_overview_model)
    def get(self):
        try:
            overview_data = market_overview_service.get_market_overview()
            return overview_data, 200
        except Exception as e:
            current_app.logger.error(f"Error fetching market overview: {e}", exc_info=True)
            return {"message": f"An error occurred while retrieving market overview: {str(e)}"}, 500


