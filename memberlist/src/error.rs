use thiserror::Error;

#[derive(Error, Debug)]
pub enum MemberListError {
    #[error("Unexpected internal error")]
    UnexpectedInternalError,
    #[error("The member list does not start")]
    NotStartError,
}
