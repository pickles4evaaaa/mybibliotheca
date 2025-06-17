from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, BooleanField, SubmitField, TextAreaField
from wtforms.validators import DataRequired, Email, EqualTo, Length, ValidationError
from .models import User

def validate_strong_password(form, field):
    """Custom validator for strong passwords"""
    if not User.is_password_strong(field.data):
        requirements = User.get_password_requirements()
        raise ValidationError(f"Password must meet the following requirements: {'; '.join(requirements)}")

class LoginForm(FlaskForm):
    username = StringField('Username or Email', validators=[DataRequired()])
    password = PasswordField('Password', validators=[DataRequired()])
    remember_me = BooleanField('Remember Me')
    submit = SubmitField('Sign In')

class RegistrationForm(FlaskForm):
    username = StringField('Username', validators=[
        DataRequired(), 
        Length(min=3, max=20, message='Username must be between 3 and 20 characters')
    ])
    email = StringField('Email', validators=[DataRequired(), Email()])
    password = PasswordField('Password', validators=[
        DataRequired(),
        validate_strong_password
    ])
    password2 = PasswordField('Repeat Password', validators=[
        DataRequired(), 
        EqualTo('password', message='Passwords must match')
    ])
    submit = SubmitField('Create User')

    def validate_username(self, username):
        user = User.query.filter_by(username=username.data).first()
        if user is not None:
            raise ValidationError('Please use a different username.')

    def validate_email(self, email):
        user = User.query.filter_by(email=email.data).first()
        if user is not None:
            raise ValidationError('Please use a different email address.')

class UserProfileForm(FlaskForm):
    username = StringField('Username', validators=[
        DataRequired(), 
        Length(min=3, max=20, message='Username must be between 3 and 20 characters')
    ])
    email = StringField('Email', validators=[DataRequired(), Email()])
    submit = SubmitField('Update Profile')

    def __init__(self, original_username, original_email, *args, **kwargs):
        super(UserProfileForm, self).__init__(*args, **kwargs)
        self.original_username = original_username
        self.original_email = original_email

    def validate_username(self, username):
        if username.data != self.original_username:
            user = User.query.filter_by(username=username.data).first()
            if user is not None:
                raise ValidationError('Please use a different username.')

    def validate_email(self, email):
        if email.data != self.original_email:
            user = User.query.filter_by(email=email.data).first()
            if user is not None:
                raise ValidationError('Please use a different email address.')

class ChangePasswordForm(FlaskForm):
    current_password = PasswordField('Current Password', validators=[DataRequired()])
    new_password = PasswordField('New Password', validators=[
        DataRequired(),
        validate_strong_password
    ])
    new_password2 = PasswordField('Repeat New Password', validators=[
        DataRequired(), 
        EqualTo('new_password', message='Passwords must match')
    ])
    submit = SubmitField('Change Password')

class PrivacySettingsForm(FlaskForm):
    share_current_reading = BooleanField('Share what I\'m currently reading')
    share_reading_activity = BooleanField('Share my reading activity and statistics')
    share_library = BooleanField('Make my library visible to others')
    submit = SubmitField('Update Privacy Settings')

class AdminPasswordResetForm(FlaskForm):
    new_password = PasswordField('New Password', validators=[
        DataRequired(),
        validate_strong_password
    ])
    new_password2 = PasswordField('Repeat New Password', validators=[
        DataRequired(), 
        EqualTo('new_password', message='Passwords must match')
    ])
    force_change = BooleanField('Require user to change password on next login', default=True)
    submit = SubmitField('Reset User Password')

class ForcedPasswordChangeForm(FlaskForm):
    new_password = PasswordField('New Password', validators=[
        DataRequired(),
        validate_strong_password
    ])
    new_password2 = PasswordField('Repeat New Password', validators=[
        DataRequired(), 
        EqualTo('new_password', message='Passwords must match')
    ])
    submit = SubmitField('Set New Password')
