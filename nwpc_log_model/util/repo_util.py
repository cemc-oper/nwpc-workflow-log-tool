# coding=utf-8


from nwpc_log_model.rdbms_model.models import User, Repo, SmsRepo, Record


class RepoUtil(object):
    @staticmethod
    def create_owner(owner, session):
        owner_object = User()
        owner_object.user_name = owner
        result = session.query(User).filter(User.user_name == owner) \
            .first()
        if not result:
            owner_object = session.merge(owner_object)
            session.commit()
        else:
            owner_object = result
        return owner_object

    @staticmethod
    def create_sms_repo(owner, repo, session):
        owner_object = RepoUtil.create_owner(owner, session)

        repo_object = Repo()
        repo_object.repo_name = repo
        repo_object.repo_type = 'sms'
        repo_object.user_id = owner_object.user_id

        result = session.query(Repo).filter(Repo.repo_name == repo)\
            .filter(owner_object.user_id == Repo.user_id) \
            .first()
        if not result:
            session.add(repo_object)
            session.commit()

        result = session.query(SmsRepo).filter(SmsRepo.repo_id == repo_object.repo_id).first()
        if not result:
            sms_repo_object = SmsRepo()
            sms_repo_object.repo_name = repo
            sms_repo_object.repo_id = repo_object.repo_id
            sms_repo_object.user_id = repo_object.user_id
            session.add(sms_repo_object)
            session.commit()

        Record.prepare(owner, repo)
        if not Record.__table__.exists(bind=session.get_bind()):
            RepoUtil.create_record_table(owner, repo, session)

    @staticmethod
    def create_record_table(owner, repo, session):
        Record.prepare(owner, repo)
        Record.__table__.create(bind=session.get_bind(), checkfirst=True)
        Record.init()
