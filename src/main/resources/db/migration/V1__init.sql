create table tasks (
                       id bigserial primary key,
                       type varchar(255) not null,
                       payload text not null,
                       status varchar(50) not null,
                       created_at timestamp with time zone not null,
                       updated_at timestamp with time zone,
                       version bigint
);

create index idx_tasks_status_created_at
    on tasks (status, created_at);
