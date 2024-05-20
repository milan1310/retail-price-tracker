from flask_marshmallow import Marshmallow
from .models import User,Client,Project, Timesheet
from .models import User, Task
from marshmallow import fields

ma = Marshmallow()

class ApproverSchema(ma.Schema):
    id = fields.Integer()
    name = fields.String()

class User_detailes(ma.Schema):
    id = fields.Integer()
    name = fields.String()
    email = fields.String()
    role = fields.String()
    team_id = fields.Integer(attribute="team.id")
    team_name = fields.String(attribute="team.name")
    supervisor_id = fields.Integer(attribute="supervisor.id")
    supervisor_name = fields.String(attribute="supervisor.name")
    approver_id = fields.Integer(attribute="approver.id")
    approver_name = fields.String(attribute="approver.name")
    is_deleted = fields.Boolean()
    is_active=fields.Boolean()
    company_id = fields.Integer()
    workspace_name = fields.String()
    profile_url = fields.String()
    guide_me = fields.Boolean()
    guide_me_status=fields.String()

class Client_details(ma.SQLAlchemyAutoSchema):
    class Meta:
        include_fk = True
        model = Client
        strict = True
        load_instance = True
        fields = ("id","name","email","contact","is_deleted","is_active","company_id")

class Project_details(ma.SQLAlchemyAutoSchema):
    
    id=fields.Integer()
    name=fields.String()
    start_date=fields.Date()
    end_date=fields.Date()
    client_id=fields.Integer(attribute="client.id")
    is_deleted=fields.Boolean()
    is_active=fields.Boolean()
    client_name=fields.String(attribute="client.name")

        

class Task_details(ma.Schema):
    id = fields.Integer()
    name = fields.String()
    start_date = fields.Date()
    end_date = fields.Date()
    estimated_hours = fields.Float()
    project_id = fields.Integer()
    client_id = fields.Integer()
    is_active = fields.Boolean()
    is_deleted = fields.Bool()
    description = fields.String()
    project_name = fields.String(attribute="project.name")
    client_name = fields.String(attribute="client.name")
    
class Timesheet_details(ma.SQLAlchemyAutoSchema):
    class Meta:
        include_fk = True
        model = Timesheet
        strict = True
        load_instance = True
        fields = ("id","user_id","timesheet_name","start_date","end_date", "status", "is_deleted", "is_recallable", "rejection_count")

# class Approver_details(ma.SQLAlchemyAutoSchema):
#     class Meta:
#         include_fk = True
#         model = User
#         strict = True
#         load_instance = True
#         fields = ("id","name")

# class Approver_report(ma.Schema):




class TimesheetSchema(ma.Schema):
    id = fields.Integer()
    user_id = fields.Integer()
    timesheet_name = fields.String()
    start_date = fields.Date()
    end_date = fields.Date()


class ProjectCombine(ma.Schema):
    project = fields.Nested(Project_details)
 

class UserSchema(ma.Schema):
    id = fields.Integer()
    name = fields.String()
    approver = fields.Nested(ApproverSchema)
    supervisor = fields.Nested(ApproverSchema)
class TimesheetSchema(ma.Schema):
    id = fields.Integer()
    user_id = fields.Integer()
    timesheet_name = fields.String()
    start_date = fields.Date()
    end_date = fields.Date()

class CombinedSchema(ma.Schema):
    timesheet_name = fields.String()
    employee_name = fields.String(attribute="user.name")
    supervisor = fields.String(attribute="user.supervisor.name")
    approver = fields.String(attribute="user.approver.name")
    start_date = fields.Date()
    end_date = fields.Date()
    id = fields.Integer()
    approver_id = fields.Integer(attribute="user.approver.id")
    supervisor_id = fields.Integer(attribute="user.supervisor.id")
    employee_id = fields.Integer(attribute="user.id")
    is_recallable = fields.Boolean()
    is_deleted = fields.Boolean()
    status = fields.String()

class Team_details(ma.Schema):
    id = fields.Integer()
    name = fields.String()
    company_id = fields.Integer()
    is_deleted = fields.Boolean()