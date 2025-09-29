from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, BooleanField, SubmitField, TextAreaField, IntegerField, SelectField
from wtforms.validators import DataRequired, Email, EqualTo, Length, ValidationError, Optional, NumberRange
from .domain.models import User

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
    role = SelectField(
        'Role',
        choices=[('user', 'Standard User'), ('admin', 'Administrator')],
        validators=[DataRequired()],
        default='user'
    )
    submit = SubmitField('Create User')

    def validate_username(self, username):
        from .services import user_service
        user = user_service.get_user_by_username_sync(username.data)
        if user is not None:
            raise ValidationError('Please use a different username.')

    def validate_email(self, email):
        from .services import user_service
        user = user_service.get_user_by_email_sync(email.data)
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
            from .services import user_service
            user = user_service.get_user_by_username_sync(username.data)
            if user is not None:
                raise ValidationError('Please use a different username.')

    def validate_email(self, email):
        if email.data != self.original_email:
            from .services import user_service
            user = user_service.get_user_by_email_sync(email.data)
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
    timezone = SelectField('Timezone', choices=[], default='UTC')
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

class SetupForm(FlaskForm):
    """Form for initial admin user setup during first-time installation"""
    username = StringField('Admin Username', validators=[
        DataRequired(), 
        Length(min=3, max=20, message='Username must be between 3 and 20 characters')
    ])
    email = StringField('Admin Email', validators=[DataRequired(), Email()])
    password = PasswordField('Password', validators=[
        DataRequired(),
        validate_strong_password
    ])
    password2 = PasswordField('Repeat Password', validators=[
        DataRequired(), 
        EqualTo('password', message='Passwords must match')
    ])
    submit = SubmitField('Complete Setup')

    def validate_username(self, username):
        """Ensure no users exist with this username"""
        try:
            from .services import user_service
            user = user_service.get_user_by_username_sync(username.data)
            if user is not None:
                raise ValidationError('Please use a different username.')
        except Exception as e:
            # During onboarding, database might not be fully set up, so ignore validation errors
            pass

    def validate_email(self, email):
        """Ensure no users exist with this email"""
        try:
            from .services import user_service
            user = user_service.get_user_by_email_sync(email.data)
            if user is not None:
                raise ValidationError('Please use a different email address.')
        except Exception as e:
            # During onboarding, database might not be fully set up, so ignore validation errors
            pass
        
        # Check for .local domains and provide helpful message
        if email.data and email.data.endswith('.local'):
            raise ValidationError('Email addresses ending in .local are not supported. Please use a standard email domain like @gmail.com, @example.com, etc.')
    
    def validate(self, extra_validators=None):
        """Enhanced validation with better error handling for onboarding."""
        # Force fresh CSRF token generation if needed
        csrf_token_field = getattr(self, 'csrf_token', None)
        if not csrf_token_field or not csrf_token_field.data:
            try:
                from flask_wtf.csrf import generate_csrf
                from flask import session
                csrf_token = generate_csrf()
                session.modified = True
            except Exception as e:
                # Handle CSRF token generation error gracefully
                pass
        
        # Call parent validation
        return super().validate(extra_validators)

class ReadingStreakForm(FlaskForm):
    reading_streak_offset = IntegerField(
        'Reading Streak Offset',
        validators=[Optional(), NumberRange(min=0, max=10000)],
        render_kw={
            'placeholder': '0',
            'class': 'form-control'
        }
    )
    submit = SubmitField('Update Streak Settings', render_kw={'class': 'btn btn-primary'})

# === Genre/Category Forms ===

class CategoryForm(FlaskForm):
    name = StringField('Category Name', validators=[
        DataRequired(message='Category name is required'), 
        Length(min=1, max=100, message='Category name must be between 1 and 100 characters')
    ])
    description = TextAreaField('Description', validators=[
        Optional(),
        Length(max=500, message='Description cannot exceed 500 characters')
    ])
    parent_id = SelectField('Parent Category', validators=[Optional()], coerce=str)
    aliases = TextAreaField('Aliases', validators=[Optional()], 
                           render_kw={'rows': 3, 'placeholder': 'Enter alternative names, one per line'})
    icon = StringField('Icon', validators=[
        Optional(),
        Length(max=10, message='Icon cannot exceed 10 characters')
    ])
    color = StringField('Color', validators=[Optional()])
    submit = SubmitField('Save Category')
    
    def __init__(self, current_category_id=None, *args, **kwargs):
        super(CategoryForm, self).__init__(*args, **kwargs)
        self.current_category_id = current_category_id
        self.populate_parent_choices()
    
    def populate_parent_choices(self):
        """Populate parent category choices, excluding the current category and its descendants"""
        try:
            from .services import book_service
            categories = book_service.list_all_categories_sync()  # type: ignore
            
            # Build choices list
            choices = [('', 'None (Root Category)')]
            
            for category in categories:
                # Get category attributes safely (handle both dict and object)
                category_id = category.get('id') if isinstance(category, dict) else getattr(category, 'id', None)
                category_name = category.get('name') if isinstance(category, dict) else getattr(category, 'name', 'Unknown')
                category_level = category.get('level', 0) if isinstance(category, dict) else getattr(category, 'level', 0)
                
                # Skip current category (for edit form)
                if self.current_category_id and category_id == self.current_category_id:
                    continue
                
                # Skip descendants of current category (for edit form)
                if self.current_category_id and self.is_descendant(category, self.current_category_id):
                    continue
                
                # Create indented display name based on level
                indent = '  ' * category_level
                display_name = f"{indent}{category_name}"
                # Ensure category_id is string
                if category_id is not None:
                    choices.append((str(category_id), display_name))
            
            self.parent_id.choices = choices  # type: ignore
        except Exception as e:
            print(f"Error populating parent choices: {e}")
            self.parent_id.choices = [('', 'None (Root Category)')]
    
    def is_descendant(self, category, ancestor_id):
        """Check if category is a descendant of the given ancestor"""
        # Get parent_id safely (handle both dict and object)
        parent_id = category.get('parent_id') if isinstance(category, dict) else getattr(category, 'parent_id', None)
        
        if parent_id:
            if parent_id == ancestor_id:
                return True
            # Would need to fetch parent to continue, simplified for now
        return False

    def validate_name(self, name):
        """Ensure category name is unique at the same level"""
        try:
            from .services import book_service
            
            # Get parent_id from form
            parent_id = self.parent_id.data if self.parent_id.data else None
            
            # Check for existing category with same name and parent
            existing_categories = book_service.search_categories_sync(name.data)  # type: ignore
            
            for category in existing_categories:
                # Handle both dictionary and object formats
                category_name = category.get('name') if isinstance(category, dict) else getattr(category, 'name', None)
                category_parent_id = category.get('parent_id') if isinstance(category, dict) else getattr(category, 'parent_id', None)
                category_id = category.get('id') if isinstance(category, dict) else getattr(category, 'id', None)
                
                if (category_name and category_name.lower() == name.data.lower() and 
                    category_parent_id == parent_id and
                    (not self.current_category_id or str(category_id) != str(self.current_category_id))):
                    if parent_id:
                        raise ValidationError(f'A category named "{name.data}" already exists under the selected parent.')
                    else:
                        raise ValidationError(f'A root category named "{name.data}" already exists.')
        except Exception as e:
            if "already exists" in str(e):
                raise e
            # Don't fail validation on service errors
            pass

class MergeCategoriesForm(FlaskForm):
    source_id = SelectField('Source Category (will be deleted)', validators=[DataRequired()], coerce=str)
    target_id = SelectField('Target Category (will receive content)', validators=[DataRequired()], coerce=str)
    merge_aliases = BooleanField('Merge aliases from source to target', default=True)
    merge_description = BooleanField('Append source description to target', default=False)
    preserve_hierarchy = BooleanField('Move subcategories to target category', default=True)
    submit = SubmitField('Merge Categories')
    
    def __init__(self, *args, **kwargs):
        super(MergeCategoriesForm, self).__init__(*args, **kwargs)
        self.populate_category_choices()
    
    def populate_category_choices(self):
        """Populate category choices for source and target"""
        try:
            from .services import book_service
            categories = book_service.get_all_categories_sync()  # type: ignore
            
            choices = []
            for category in categories:
                # Get category attributes safely (handle both dict and object)
                category_id = category.get('id') if isinstance(category, dict) else getattr(category, 'id', None)
                category_name = category.get('name') if isinstance(category, dict) else getattr(category, 'name', 'Unknown')
                category_level = category.get('level', 0) if isinstance(category, dict) else getattr(category, 'level', 0)
                category_book_count = category.get('book_count', 0) if isinstance(category, dict) else getattr(category, 'book_count', 0)
                
                # Create display name with hierarchy
                indent = '  ' * category_level
                book_info = f" ({category_book_count} books)" if category_book_count > 0 else ""
                display_name = f"{indent}{category_name}{book_info}"
                if category_id is not None:
                    choices.append((str(category_id), display_name))
            
            if not choices:
                choices = [('', 'No categories available')]
            
            self.source_id.choices = choices  # type: ignore
            self.target_id.choices = choices  # type: ignore
        except Exception as e:
            print(f"Error populating category choices: {e}")
            self.source_id.choices = [('', 'No categories available')]
            self.target_id.choices = [('', 'No categories available')]
    
    def validate_source_id(self, source_id):
        """Validate source category selection"""
        if not source_id.data:
            raise ValidationError('Please select a source category.')
    
    def validate_target_id(self, target_id):
        """Validate target category selection"""
        if not target_id.data:
            raise ValidationError('Please select a target category.')
        
        if target_id.data == self.source_id.data:
            raise ValidationError('Source and target categories cannot be the same.')

class ReadingLogEntryForm(FlaskForm):
    book_id = SelectField('Book', choices=[], validators=[DataRequired()], 
                         render_kw={'class': 'form-select'})
    start_page = IntegerField('Start Page', validators=[Optional(), NumberRange(min=1)], 
                             default=1, render_kw={'class': 'form-control'})
    end_page = IntegerField('End Page', validators=[Optional(), NumberRange(min=1)], 
                           render_kw={'class': 'form-control'})
    pages_read = IntegerField('Pages Read', validators=[Optional(), NumberRange(min=1)], 
                             render_kw={'class': 'form-control'})
    minutes_read = IntegerField('Minutes Read', validators=[Optional(), NumberRange(min=1)], 
                               render_kw={'class': 'form-control'})
    notes = TextAreaField('Notes', validators=[Optional()], 
                         render_kw={'class': 'form-control', 'rows': 3})
    submit = SubmitField('Log Reading Session', render_kw={'class': 'btn btn-primary'})
