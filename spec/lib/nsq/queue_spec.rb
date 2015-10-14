describe ::Queue do
  it_behaves_like 'a thread-safe queue'
end

describe Nsq::Queues::Basic do
  it_behaves_like 'a thread-safe queue'
end

describe Nsq::Queues::LockFree do
  it_behaves_like 'a thread-safe queue'
end
